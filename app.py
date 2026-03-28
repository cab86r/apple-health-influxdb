"""
Apple Health to InfluxDB 2.x Ingester v5.0
支援 Health Auto Export JSON 格式 + Apple Shortcuts 格式
完整資料收集：一般指標、運動總結、GPS 軌跡、高解析度心率
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
        logger.error(f"❌ InfluxDB 建立失敗: {e}")
        return None


def on_success(conf, data: str):
    pass


def on_error(conf, data: str, exception: Exception):
    logger.error(f"🚨 InfluxDB 拒絕資料! 原因: {exception}")


def on_retry(conf, data: str, exception: Exception):
    logger.warning(f"⚠️ 寫入重試中... 原因: {exception}")


def process_data_background(data, target_name):
    """在背景執行緒中處理解析與寫入"""
    client = get_influx_client()
    if not client:
        return
    
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
        # 格式 1: Health Auto Export JSON 格式
        # ==========================================
        if isinstance(data, dict) and 'data' in data:
            
            # --- 1. 處理一般連續性數據 (Metrics) ---
            for metric in data['data'].get('metrics', []):
                metric_name = metric.get('name', 'unknown')
                unit = metric.get('unit', '')
                
                for datapoint in metric.get('data', []):
                    date_str = datapoint.get('date')
                    qty = datapoint.get('qty')
                    if date_str and qty is not None:
                        fixed_date = date_str.replace(" ", "T", 1) if " " in date_str else date_str
                        measurement = f"{metric_name}_{unit}" if unit else metric_name
                        p = Point(measurement).tag("source", "apple_health").field("qty", float(qty)).time(fixed_date)
                        if target_name:
                            p = p.tag("target", target_name)
                        points.append(p)
            
            # --- 2. 處理運動紀錄 (Workouts) ---
            for workout in data['data'].get('workouts', []):
                workout_name = workout.get('name', 'unknown')
                start_date = workout.get('start', workout.get('startDate'))
                
                if start_date:
                    fixed_start = start_date.replace(" ", "T", 1) if " " in start_date else start_date
                    
                    # 2-A: 寫入運動總結資料 (Duration, Calories, etc.)
                    p = Point("workout").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_start)
                    if target_name:
                        p = p.tag("target", target_name)
                    
                    field_added = False
                    exclude_keys = ['name', 'start', 'end', 'startDate', 'endDate', 'route', 'heartRateData']
                    for key, value in workout.items():
                        if key not in exclude_keys and value is not None:
                            if isinstance(value, (int, float)):
                                p = p.field(key, float(value))
                                field_added = True
                    if field_added:
                        points.append(p)

                    # 2-B: 寫入 GPS 軌跡 (Route)
                    route_data = workout.get('route', [])
                    if isinstance(route_data, list) and len(route_data) > 0:
                        for pt in route_data:
                            lat = pt.get('lat')
                            lon = pt.get('lon')
                            alt = pt.get('altitude')
                            pt_time = pt.get('timestamp')
                            if lat is not None and lon is not None and pt_time:
                                fixed_pt_time = pt_time.replace(" ", "T", 1) if " " in pt_time else pt_time
                                rp = Point("workout_route") \
                                    .tag("source", "apple_health") \
                                    .tag("workoutActivityType", workout_name) \
                                    .time(fixed_pt_time) \
                                    .field("lat", float(lat)) \
                                    .field("lon", float(lon))
                                if alt is not None:
                                    rp = rp.field("altitude", float(alt))
                                if target_name:
                                    rp = rp.tag("target", target_name)
                                points.append(rp)

                    # 2-C: 寫入高解析度連續心率 (Heart Rate Data)
                    hr_data = workout.get('heartRateData', [])
                    if isinstance(hr_data, list) and len(hr_data) > 0:
                        for hr in hr_data:
                            qty = hr.get('qty')
                            hr_time = hr.get('date', hr.get('timestamp'))
                            if qty is not None and hr_time:
                                fixed_hr_time = hr_time.replace(" ", "T", 1) if " " in hr_time else hr_time
                                hrp = Point("workout_heart_rate") \
                                    .tag("source", "apple_health") \
                                    .tag("workoutActivityType", workout_name) \
                                    .time(fixed_hr_time) \
                                    .field("qty", float(qty))
                                if target_name:
                                    hrp = hrp.tag("target", target_name)
                                points.append(hrp)

        # ==========================================
        # 格式 2: Apple Shortcuts 捷徑格式 (完整加回)
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
        
        # 執行批次寫入
        if points:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            logger.info(f"✅ 成功排程寫入 {len(points)} 個資料點 (包含 GPS 軌跡與心率流)。")
    
    except Exception as e:
        logger.error(f"❌ 解析錯誤: {e}", exc_info=True)
    finally:
        write_api.flush()
        write_api.close()
        client.close()


@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
@app.route('/collect', methods=['POST'])
def ingest():
    """統一入口：接收資料並交給背景線程處理"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"status": "error", "message": "無效的 JSON"}), 400
    executor.submit(process_data_background, data, request.args.get('target', None))
    return jsonify({"status": "processing"}), 202


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
        "version": "5.0.0 (Full Data Collection)",
        "measurements": {
            "metrics": "健康指標 (步數、心率、睡眠等)",
            "workout": "運動總結 (距離、卡路里、時長)",
            "workout_route": "GPS 軌跡 (經緯度、海拔)",
            "workout_heart_rate": "運動心率流 (每秒心率)"
        },
        "features": {
            "multi_threaded": True,
            "async_write": True,
            "batch_write": True,
            "workout_support": True,
            "gps_route_support": True,
            "heart_rate_stream": True,
            "shortcuts_support": True,
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
    logger.info(f"🚀 Apple Health Ingester v5.0 (資料不遺漏全解析版)")
    logger.info(f"📊 InfluxDB: {INFLUX_URL}")
    logger.info(f"📦 Bucket: {INFLUX_BUCKET}")
    logger.info(f"⚙️ Worker 數量: {MAX_WORKERS}")
    logger.info(f"📦 每批寫入量: {DATAPOINTS_CHUNK}")
    app.run(host='0.0.0.0', port=5354, debug=False, threaded=True)
