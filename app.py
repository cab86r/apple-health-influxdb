"""
Apple Health to InfluxDB 2.x Ingester v3.2.0
支援 Health Auto Export JSON 格式 + Apple Shortcuts 格式
非同步寫入錯誤監聽器，批次寫入優化
"""

import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient, Point
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
        return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)
    except Exception as e:
        logger.error(f"❌ InfluxDB 用戶端建立失敗: {e}")
        return None


# ==========================================
# 監聽器：專門捕捉非同步寫入的「無聲錯誤」
# ==========================================
def on_success(conf, data: str):
    pass  # 成功批次寫入時不特別刷 Log


def on_error(conf, data: str, exception: Exception):
    logger.error(f"🚨 InfluxDB 拒絕了這批資料! 原因: {exception}")
    logger.debug(f"被拒絕的資料內容前 200 字元: {data[:200]}")


def on_retry(conf, data: str, exception: Exception):
    logger.warning(f"⚠️ InfluxDB 寫入重試中... 原因: {exception}")


def process_data_background(data, target_name):
    """在背景執行緒中處理解析與寫入"""
    client = get_influx_client()
    if not client:
        return
    
    # 恢復「非同步批次處理」以確保效能，並掛上錯誤監聽器
    write_api = client.write_api(
        write_options=WriteOptions(
            batch_size=DATAPOINTS_CHUNK,
            flush_interval=1000,
            jitter_interval=2000,
            retry_interval=5000,
            max_retries=3
        ),
        success_callback=on_success,
        error_callback=on_error,
        retry_callback=on_retry
    )
    
    points = []
    
    try:
        # ==========================================
        # 格式 1: Health Auto Export 格式
        # ==========================================
        if isinstance(data, dict) and 'data' in data:
            # 處理一般 metrics
            for metric in data['data'].get('metrics', []):
                metric_name = metric.get('name', 'unknown')
                unit = metric.get('unit', '')
                
                for datapoint in metric.get('data', []):
                    date_str = datapoint.get('date')
                    qty = datapoint.get('qty')
                    if date_str and qty is not None:
                        # 修復時間格式 (如果有空白，替換為 T 以符合 ISO8601)
                        fixed_date = date_str.replace(" ", "T", 1) if " " in date_str else date_str
                        
                        measurement = f"{metric_name}_{unit}" if unit else metric_name
                        p = Point(measurement) \
                            .tag("source", "apple_health") \
                            .field("qty", float(qty)) \
                            .time(fixed_date)
                        if target_name:
                            p = p.tag("target", target_name)
                        points.append(p)
            
            # 處理 workouts
            for workout in data['data'].get('workouts', []):
                workout_name = workout.get('name', 'unknown')
                start_date = workout.get('start', workout.get('startDate'))
                
                if start_date:
                    fixed_start = start_date.replace(" ", "T", 1) if " " in start_date else start_date
                    
                    p = Point("workout") \
                        .tag("source", "apple_health") \
                        .tag("workoutActivityType", workout_name) \
                        .time(fixed_start)
                    
                    if target_name:
                        p = p.tag("target", target_name)
                    
                    field_added = False
                    exclude_keys = ['name', 'start', 'end', 'startDate', 'endDate', 'route', 'heartRateData']
                    for key, value in workout.items():
                        if key not in exclude_keys and value is not None:
                            if isinstance(value, (int, float)):
                                p = p.field(key, float(value))
                                field_added = True
                    
                    # 確保至少有一個數值才寫入
                    if field_added:
                        points.append(p)
        
        # ==========================================
        # 格式 2: Apple 捷徑 (Shortcuts)
        # ==========================================
        elif isinstance(data, list):
            for item in data:
                metric_name = item.get('name', item.get('metric', 'unknown'))
                unit = item.get('unit', '')
                qty = item.get('qty', item.get('value'))
                date_str = item.get('date', item.get('time'))
                
                if date_str and qty is not None:
                    fixed_date = date_str.replace(" ", "T", 1) if " " in date_str else date_str
                    measurement = f"{metric_name}_{unit}" if unit else metric_name
                    p = Point(measurement) \
                        .tag("source", "apple_shortcuts") \
                        .field("qty", float(qty)) \
                        .time(fixed_date)
                    if target_name:
                        p = p.tag("target", target_name)
                    points.append(p)
        
        else:
            logger.warning("⚠️ 收到未知格式的 JSON，放棄處理。")
            return
        
        # 丟進排程隊列
        if points:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            logger.info(f"✅ 成功將 {len(points)} 個資料點交給非同步引擎處理 (含 Workout)。")
    
    except Exception as e:
        logger.error(f"❌ 解析資料發生錯誤: {e}", exc_info=True)
    finally:
        # 強制將緩衝區內還沒送出的資料沖刷 (Flush) 到資料庫，然後關閉
        write_api.flush()
        write_api.close()
        client.close()


@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
@app.route('/collect', methods=['POST'])
def ingest():
    """統一入口：接收資料並交給背景線程處理"""
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"status": "error", "message": "無效的 JSON"}), 400
        
        target_name = request.args.get('target', None)
        executor.submit(process_data_background, data, target_name)
        
        # 手機端直接收到 202 Accepted，完全不用等
        return jsonify({"status": "processing"}), 202
    
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
            return jsonify({"status": "healthy"}), 200
    return jsonify({"status": "unhealthy"}), 503


@app.route('/', methods=['GET'])
def index():
    """首頁"""
    return jsonify({
        "service": "Apple Health to InfluxDB 2.x Ingester",
        "version": "3.2.0 (Async Safe)",
        "features": {
            "multi_threaded": True,
            "async_write": True,
            "batch_write": True,
            "workout_support": True,
            "error_callbacks": True
        },
        "endpoints": {
            "collect": "/collect",
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
    logger.info(f"🚀 啟動 Apple Health Ingester v3.2 (非同步攔截版)")
    logger.info(f"📊 InfluxDB: {INFLUX_URL}")
    logger.info(f"📦 Bucket: {INFLUX_BUCKET}")
    logger.info(f"⚙️ Worker 數量: {MAX_WORKERS}")
    logger.info(f"📦 每批寫入量: {DATAPOINTS_CHUNK}")
    app.run(host='0.0.0.0', port=5354, debug=False, threaded=True)
