"""
Apple Health to InfluxDB 2.x Ingester v8.0
Flask + Queue 背景處理架構
智慧解包巢狀 JSON，自動單位轉換
"""

import os
import sys
import socket
import logging
import threading
import queue
import time
from flask import request, Flask, jsonify
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 容錯引入 geolib (用於計算 GPS geohash)
try:
    from geolib import geohash
except ImportError:
    geohash = None

# --- 設定區 ---
INFLUX_HOST = os.getenv('INFLUX_HOST', 'localhost')
INFLUX_PORT = os.getenv('INFLUX_PORT', '8086')
INFLUX_URL = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', 'your-token-here')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'unifi')
INFLUX_BUCKET = os.getenv('INFLUX_DB', 'apple-health-v2')
DATAPOINTS_CHUNK = int(os.getenv('DATAPOINTS_CHUNK', '10000'))

# 時區設定
target_timezone = os.getenv('TZ', 'Asia/Taipei')
os.environ['TZ'] = target_timezone
try:
    time.tzset()
except AttributeError:
    pass

# --- Log 設定 ---
logger = logging.getLogger("console-output")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)
job_queue = queue.Queue()


def get_influx_client():
    """建立 InfluxDB 2.x 連線"""
    try:
        return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)
    except Exception as e:
        logger.error(f"❌ InfluxDB 建立失敗: {e}")
        return None


def safe_float(val):
    """安全轉換數值，失敗則回傳 None"""
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def process_data_worker():
    """背景處理緒：從 Queue 取出資料並寫入 InfluxDB 2.x"""
    client = get_influx_client()
    if not client:
        return
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    logger.info(f"Worker Thread Started. Target DB: {INFLUX_URL} bucket: {INFLUX_BUCKET}")
    
    while True:
        try:
            healthkit_data = job_queue.get()
            logger.info(f"Worker: 收到新任務 (佇列剩餘: {job_queue.qsize()}) - 開始處理...")
            
            start_time = time.time()
            points_buffer = []
            total_points_written = 0

            def flush_to_db(pts):
                if not pts:
                    return 0
                count = len(pts)
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=pts)
                    logger.info(f"Worker: >> 成功同步寫入 {count} 筆")
                    return count
                except Exception as e:
                    logger.error(f"Worker: !! 寫入 DB 失敗 (這批資料被丟棄): {e}")
                    return 0

            if not isinstance(healthkit_data, dict):
                job_queue.task_done()
                continue

            data_payload = healthkit_data.get("data", {})
            
            # ==========================================
            # 1. 處理 Metrics
            # ==========================================
            for metric in data_payload.get("metrics", []):
                metric_name = metric.get("name", "unknown")
                unit = metric.get("unit", "")
                measurement = f"{metric_name}_{unit}" if unit else metric_name
                
                for dp in metric.get("data", []):
                    date_str = dp.get("date")
                    if not date_str:
                        continue
                    
                    fixed_date = date_str.replace(" ", "T", 1) if " " in date_str else date_str
                    p = Point(measurement).tag("source", "apple_health").time(fixed_date)
                    
                    field_added = False
                    for k, v in dp.items():
                        if k == "date" or v is None:
                            continue
                        
                        val_f = safe_float(v)
                        if val_f is not None:
                            p = p.field(k, val_f)
                            field_added = True
                        else:
                            p = p.tag(k, str(v))

                    if field_added:
                        points_buffer.append(p)
                    if len(points_buffer) >= DATAPOINTS_CHUNK:
                        total_points_written += flush_to_db(points_buffer)
                        points_buffer = []

            # ==========================================
            # 2. 處理 Workouts (智慧解包巢狀 JSON)
            # ==========================================
            for workout in data_payload.get("workouts", []):
                workout_name = workout.get("name", "unknown")
                start_date = workout.get("start", workout.get("startDate"))
                if not start_date:
                    continue
                
                fixed_start = start_date.replace(" ", "T", 1) if " " in start_date else start_date
                
                # 2-A: 運動摘要
                p = Point("workout").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_start)
                
                field_added = False
                exclude_keys = ['name', 'start', 'end', 'startDate', 'endDate', 'route', 'heartRateData']
                
                for k, v in workout.items():
                    if k in exclude_keys or v is None:
                        continue
                    
                    # 1. 如果本來就是純數字
                    if isinstance(v, (int, float)):
                        p = p.field(k, float(v))
                        field_added = True
                    
                    # 2. 如果是字典 {'qty': ..., 'units': ...}
                    elif isinstance(v, dict):
                        # 心率特例 (包含 avg, max, min)
                        if k == "heartRate" and "avg" in v:
                            for sub_k, mapped_k in [("avg", "averageHeartRate"), ("max", "maxHeartRate"), ("min", "minHeartRate")]:
                                if sub_k in v and isinstance(v[sub_k], dict) and "qty" in v[sub_k]:
                                    val = safe_float(v[sub_k]["qty"])
                                    if val is not None:
                                        p = p.field(mapped_k, val)
                                        field_added = True
                        
                        # 常規的數值 + 單位字典
                        elif "qty" in v:
                            val = safe_float(v["qty"])
                            unit = v.get("units", "")
                            if val is not None:
                                # 自動轉換單位以配合 Grafana
                                if unit == "kJ":
                                    val /= 4.184  # kJ 轉 kcal
                                elif unit == "km":
                                    val *= 1000.0  # km 轉 m
                                p = p.field(k, val)
                                field_added = True
                    
                    # 3. 如果是陣列清單，略過不存進摘要中 (以防塞爆)
                    elif isinstance(v, list):
                        continue
                    
                    # 4. 其他字串
                    else:
                        val_f = safe_float(v)
                        if val_f is not None:
                            p = p.field(k, val_f)
                            field_added = True
                        elif k not in ["source", "sourceName", "sourceVersion", "device"]:
                            p = p.tag(k, str(v))

                if field_added:
                    points_buffer.append(p)

                # 2-B: GPS 軌跡
                for gps_point in workout.get("route", []):
                    lat = gps_point.get("lat")
                    lon = gps_point.get("lon")
                    pt_time = gps_point.get("timestamp")
                    
                    if lat is not None and lon is not None and pt_time:
                        fixed_pt_time = pt_time.replace(" ", "T", 1) if " " in pt_time else pt_time
                        rp = Point("workout_route") \
                            .tag("source", "apple_health") \
                            .tag("workoutActivityType", workout_name) \
                            .time(fixed_pt_time) \
                            .field("lat", float(lat)) \
                            .field("lon", float(lon))
                        
                        if gps_point.get("altitude") is not None:
                            rp = rp.field("altitude", float(gps_point["altitude"]))
                        if geohash:
                            rp = rp.field("geohash", geohash.encode(float(lat), float(lon), 7))
                        
                        points_buffer.append(rp)

                # 2-C: 高頻心率流
                for hr in workout.get('heartRateData', []):
                    qty = hr.get('qty')
                    hr_time = hr.get('date', hr.get('timestamp'))
                    if qty is not None and hr_time:
                        fixed_hr_time = hr_time.replace(" ", "T", 1) if " " in hr_time else hr_time
                        val_f = safe_float(qty)
                        if val_f is not None:
                            hrp = Point("workout_heart_rate") \
                                .tag("source", "apple_health") \
                                .tag("workoutActivityType", workout_name) \
                                .time(fixed_hr_time) \
                                .field("qty", val_f)
                            points_buffer.append(hrp)

                if len(points_buffer) >= DATAPOINTS_CHUNK:
                    total_points_written += flush_to_db(points_buffer)
                    points_buffer = []

            # 3. 收尾寫入
            if points_buffer:
                total_points_written += flush_to_db(points_buffer)

            duration = time.time() - start_time
            logger.info(f"Worker: 任務完成! 總計寫入 {total_points_written} 筆 (耗時 {duration:.2f} 秒)")
        
        except Exception as e:
            logger.exception("Worker Thread 發生未預期錯誤，正在重試...")
            time.sleep(1)
        finally:
            job_queue.task_done()


# 啟動背景 Worker
worker_thread = threading.Thread(target=process_data_worker, daemon=True)
worker_thread.start()


@app.route('/collect', methods=['POST'])
@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
def collect():
    """統一入口：接收資料並放入佇列"""
    try:
        healthkit_data = request.get_json(force=True, silent=True)
        if not healthkit_data:
            return jsonify({"status": "error", "message": "Empty JSON"}), 400

        job_queue.put(healthkit_data)
        logger.info("HTTP: 收到請求並加入佇列")
        return jsonify({"status": "processing", "message": "Queued"}), 202

    except Exception as e:
        logger.exception("Server Error")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """健康檢查端點"""
    return "OK", 200


@app.route('/', methods=['GET'])
def index():
    """首頁"""
    return jsonify({
        "service": "Apple Health to InfluxDB 2.x Ingester",
        "version": "8.0.0 (Smart Unpack)",
        "architecture": {
            "mode": "Flask + Queue Background Worker",
            "write_mode": "SYNCHRONOUS"
        },
        "measurements": {
            "metrics": "健康指標",
            "workout": "運動總結 (智慧解包)",
            "workout_route": "GPS 軌跡 (含 geohash)",
            "workout_heart_rate": "運動心率流"
        },
        "features": {
            "queue_processing": True,
            "smart_unpack": True,
            "unit_conversion": "kJ→kcal, km→m",
            "heart_rate_mapping": "avg/max/min",
            "gps_geohash": geohash is not None
        },
        "endpoints": {
            "collect": "/collect",
            "ingest": "/api/healthautoexport/v1/influxdb/ingest",
            "health": "/health"
        },
        "config": {
            "chunk_size": DATAPOINTS_CHUNK,
            "bucket": INFLUX_BUCKET
        }
    }), 200


if __name__ == "__main__":
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"🚀 Apple Health Ingester v8.0 (智慧解包完美相容版)")
    logger.info(f"📊 InfluxDB: {INFLUX_URL}")
    logger.info(f"📦 Bucket: {INFLUX_BUCKET}")
    logger.info(f"Dev Server: http://{ip_address}:5354/collect")
    app.run(host='0.0.0.0', port=5354, debug=False)
