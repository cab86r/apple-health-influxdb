"""
Apple Health to InfluxDB 2.x Ingester v3.1.0
支援 Health Auto Export JSON 格式 + Apple Shortcuts 格式
多線程非同步處理，批次寫入優化，修復 Workout 支援
"""

import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Thread:%(threadName)s] %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# InfluxDB 2.x 設定
INFLUX_URL = f"http://{os.getenv('INFLUX_HOST', 'localhost')}:{os.getenv('INFLUX_PORT', '8086')}"
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'unifi')
INFLUX_BUCKET = os.getenv('INFLUX_DB', 'apple-health-v2')
DATAPOINTS_CHUNK = int(os.getenv('DATAPOINTS_CHUNK', '5000'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))

# 建立多線程執行池
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="IngestWorker")


def get_influx_client():
    """建立 InfluxDB 2.x 用戶端連線"""
    try:
        client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            timeout=30000
        )
        return client
    except Exception as e:
        logger.error(f"❌ InfluxDB 用戶端建立失敗: {e}")
        return None


def process_data_background(data, target_name):
    """在背景執行緒中處理解析與寫入"""
    client = get_influx_client()
    if not client:
        return
    
    # 啟用非同步批次寫入 (Batching)
    write_api = client.write_api(write_options=WriteOptions(
        batch_size=DATAPOINTS_CHUNK,
        flush_interval=1000,
        jitter_interval=2000,
        retry_interval=5000,
        max_retries=3
    ))
    
    points = []
    
    try:
        # ==========================================
        # 格式 1: Health Auto Export 格式
        # ==========================================
        if isinstance(data, dict) and 'data' in data:
            # 處理一般連續性數據 (metrics)
            for metric in data['data'].get('metrics', []):
                metric_name = metric.get('name', 'unknown')
                unit = metric.get('unit', '')
                
                for datapoint in metric.get('data', []):
                    date = datapoint.get('date')
                    qty = datapoint.get('qty')
                    if date and qty is not None:
                        measurement = f"{metric_name}_{unit}" if unit else metric_name
                        p = Point(measurement) \
                            .tag("source", "apple_health") \
                            .field("qty", float(qty)) \
                            .time(date)  # 拿掉 WritePrecision.NS 交給客戶端自動判斷 ISO 格式
                        if target_name:
                            p = p.tag("target", target_name)
                        points.append(p)
            
            # 處理運動紀錄 (workouts) - 【已修復三個 Bug】
            for workout in data['data'].get('workouts', []):
                workout_name = workout.get('name', 'unknown')
                # 修復 Bug 1: 完美相容 HAE 的 'start' 以及其他版本的 'startDate'
                start_date = workout.get('start', workout.get('startDate'))
                
                if start_date:
                    p = Point("workout") \
                        .tag("source", "apple_health") \
                        .tag("workoutActivityType", workout_name) \
                        .time(start_date)
                    
                    if target_name:
                        p = p.tag("target", target_name)
                    
                    field_added = False
                    # 修復 Bug 3: 動態塞入所有數值，並排除無效欄位
                    exclude_keys = ['name', 'start', 'end', 'startDate', 'endDate', 'route', 'heartRateData']
                    for key, value in workout.items():
                        if key not in exclude_keys and value is not None:
                            if isinstance(value, (int, float)):
                                p = p.field(key, float(value))
                                field_added = True
                    
                    # 確保至少有一個數值才寫入，否則 InfluxDB 會報錯
                    if field_added:
                        points.append(p)
        
        # ==========================================
        # 格式 2: Apple 捷徑 (Shortcuts) 扁平化格式
        # ==========================================
        elif isinstance(data, list):
            for item in data:
                metric_name = item.get('name', item.get('metric', 'unknown'))
                unit = item.get('unit', '')
                qty = item.get('qty', item.get('value'))
                date = item.get('date', item.get('time'))
                
                if date and qty is not None:
                    measurement = f"{metric_name}_{unit}" if unit else metric_name
                    p = Point(measurement) \
                        .tag("source", "apple_shortcuts") \
                        .field("qty", float(qty)) \
                        .time(date)
                    if target_name:
                        p = p.tag("target", target_name)
                    points.append(p)
        
        else:
            logger.warning("⚠️ 收到未知格式的 JSON，放棄處理。")
            return
        
        # 執行寫入
        if points:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            logger.info(f"✅ 成功排程寫入 {len(points)} 個資料點 (含 Workout)。")
    
    except Exception as e:
        logger.error(f"❌ 背景處理/寫入資料發生錯誤: {e}", exc_info=True)
    finally:
        # 確保緩衝區清空並關閉連線
        write_api.close()
        client.close()


@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
@app.route('/collect', methods=['POST'])
def ingest():
    """統一入口：接收資料並交給背景線程處理"""
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                "status": "error",
                "message": "無效的 JSON 資料或未提供 Content-Type: application/json"
            }), 400
        
        target_name = request.args.get('target', None)
        executor.submit(process_data_background, data, target_name)
        
        return jsonify({
            "status": "processing",
            "message": "資料已接收，背景非同步處理中",
            "timestamp": datetime.now().isoformat()
        }), 202
    
    except Exception as e:
        logger.error(f"❌ 接收請求失敗: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """健康檢查端點"""
    client = get_influx_client()
    if client:
        health = client.health()
        client.close()
        if health.status == "pass":
            return jsonify({"status": "healthy", "database": "connected"}), 200
    return jsonify({"status": "unhealthy", "database": "disconnected"}), 503


@app.route('/', methods=['GET'])
def index():
    """首頁"""
    return jsonify({
        "service": "Apple Health to InfluxDB 2.x Ingester",
        "version": "3.1.0",
        "features": {
            "multi_threaded": True,
            "async_write": True,
            "batch_write": True,
            "workout_support": True  # 標示已修復 Workout
        },
        "endpoints": {
            "collect": "/collect (相容舊版)",
            "ingest": "/api/healthautoexport/v1/influxdb/ingest",
            "health": "/health"
        },
        "config": {
            "max_workers": MAX_WORKERS,
            "batch_size": DATAPOINTS_CHUNK,
            "bucket": INFLUX_BUCKET
        }
    }), 200


if __name__ == '__main__':
    logger.info(f"🚀 啟動 Apple Health Ingester v3.1 (Fixed Workout Support)")
    logger.info(f"📊 InfluxDB: {INFLUX_URL}")
    logger.info(f"📦 Bucket: {INFLUX_BUCKET}")
    logger.info(f"⚙️ Worker 數量: {MAX_WORKERS}")
    logger.info(f"📦 每批寫入量: {DATAPOINTS_CHUNK}")
    app.run(host='0.0.0.0', port=5354, debug=False, threaded=True)
