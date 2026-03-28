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
DATAPOINTS_CHUNK = int(os.getenv('DATAPOINTS_CHUNK', 2000))
# 🌟 全新加入：多線程工人數量 (預設 16 個並行)
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 16))

target_timezone = os.getenv('TZ', 'Asia/Taipei')
os.environ['TZ'] = target_timezone
try:
    time.tzset()
except AttributeError:
    pass

logger = logging.getLogger("console-output")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Worker-%(threadName)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)
job_queue = queue.Queue()

def get_influx_client():
    try:
        # Timeout 延長至 60 秒，確保大批次寫入不中斷
        return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=60000)
    except Exception as e:
        logger.error(f"❌ InfluxDB 建立失敗: {e}")
        return None

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

# 🌟 改為接受 worker_id 的多線程函式
def process_data_worker(worker_id):
    # 每個 Worker 都有自己專屬的 Client，避免多線程搶奪資源
    client = get_influx_client()
    if not client:
        logger.error(f"Worker-{worker_id} 無法啟動: InfluxDB Client 初始化失敗")
        return
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    logger.info(f"啟動就緒。")
    
    while True:
        try:
            healthkit_data = job_queue.get()
            logger.info(f"收到新任務 (佇列剩餘任務數: {job_queue.qsize()}) - 開始解析...")
            
            start_time = time.time()
            points_buffer = []
            total_points_written = 0

            def flush_to_db(pts):
                if not pts: return 0
                count = len(pts)
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=pts)
                    logger.info(f">> 成功同步寫入 {count} 筆")
                    return count
                except Exception as e:
                    logger.error(f"!! 寫入 DB 失敗 (這批資料被丟棄): {e}")
                    try:
                        dump_filename = f"failed_dump_W{worker_id}_{int(time.time())}.txt"
                        with open(dump_filename, "w", encoding="utf-8") as f:
                            for p in pts: f.write(p.to_line_protocol() + "\n")
                        logger.error(f"💾 已將 {count} 筆失敗資料備份至: {dump_filename}")
                    except Exception as dump_err:
                        logger.error(f"無法備份失敗資料: {dump_err}")
                    return 0

            if not isinstance(healthkit_data, dict):
                job_queue.task_done()
                continue

            data_payload = healthkit_data.get("data", {})
            
            # --- 解析 Metrics ---
            for metric in data_payload.get("metrics", []):
                metric_name = metric.get("name", "unknown")
                unit = metric.get("unit", "")
                measurement = f"{metric_name}_{unit}" if unit else metric_name
                
                for dp in metric.get("data", []):
                    date_str = dp.get("date")
                    if not date_str: continue
                    
                    fixed_date = date_str.replace(" ", "T", 1) if " " in date_str else date_str
                    p = Point(measurement).tag("source", "apple_health").time(fixed_date)
                    
                    field_added = False
                    for k, v in dp.items():
                        if k == "date" or v is None: continue
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

            # --- 解析 Workouts ---
            for workout in data_payload.get("workouts", []):
                workout_name = workout.get("name", "unknown")
                start_date = workout.get("start", workout.get("startDate"))
                if not start_date: continue
                
                fixed_start = start_date.replace(" ", "T", 1) if " " in start_date else start_date
                
                # 2-A: 運動摘要
                p = Point("workout").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_start)
                field_added = False
                exclude_keys = ['name', 'start', 'end', 'startDate', 'endDate', 'route', 'heartRateData']
                
                for k, v in workout.items():
                    if v is None: continue
                    if isinstance(v, (int, float)):
                        p = p.field(k, float(v))
                        field_added = True
                    elif isinstance(v, dict):
                        if k == "heartRate" and "avg" in v:
                            for sub_k, mapped_k in [("avg", "averageHeartRate"), ("max", "maxHeartRate"), ("min", "minHeartRate")]:
                                if sub_k in v and isinstance(v[sub_k], dict) and "qty" in v[sub_k]:
                                    val = safe_float(v[sub_k]["qty"])
                                    if val is not None:
                                        p = p.field(mapped_k, val)
                                        field_added = True
                        elif "qty" in v:
                            val = safe_float(v["qty"])
                            unit = v.get("units", "")
                            if val is not None:
                                if unit == "kJ": val /= 4.184
                                elif unit == "km": val *= 1000.0
                                p = p.field(k, val)
                                field_added = True
                    elif isinstance(v, list):
                        continue
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
                        rp = Point("workout_route").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_pt_time).field("lat", float(lat)).field("lon", float(lon))
                        if gps_point.get("altitude") is not None: rp = rp.field("altitude", float(gps_point["altitude"]))
                        if geohash: rp = rp.field("geohash", geohash.encode(float(lat), float(lon), 7))
                        points_buffer.append(rp)

                # 2-C: 心率流
                for hr in workout.get('heartRateData', []):
                    qty = hr.get('qty')
                    hr_time = hr.get('date', hr.get('timestamp'))
                    if qty is not None and hr_time:
                        fixed_hr_time = hr_time.replace(" ", "T", 1) if " " in hr_time else hr_time
                        val_f = safe_float(qty)
                        if val_f is not None:
                            hrp = Point("workout_heart_rate").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_hr_time).field("qty", val_f)
                            points_buffer.append(hrp)

                # 2-D: 萬用陣列拆解 (activeEnergy, distance 等)
                for k, v in workout.items():
                    if isinstance(v, list) and k not in ['route', 'heartRateData']:
                        for item in v:
                            if isinstance(item, dict):
                                qty = item.get('qty')
                                pt_time = item.get('date', item.get('timestamp'))
                                if qty is not None and pt_time:
                                    fixed_pt_time = pt_time.replace(" ", "T", 1) if " " in pt_time else pt_time
                                    val_f = safe_float(qty)
                                    unit = item.get('units', '')
                                    if val_f is not None:
                                        if unit == "kJ": val_f /= 4.184
                                        elif unit == "km": val_f *= 1000.0
                                        dp = Point(f"workout_{k}").tag("source", "apple_health").tag("workoutActivityType", workout_name).time(fixed_pt_time).field("qty", val_f)
                                        points_buffer.append(dp)

                if len(points_buffer) >= DATAPOINTS_CHUNK:
                    total_points_written += flush_to_db(points_buffer)
                    points_buffer = []

            # --- 收尾寫入 ---
            if points_buffer:
                total_points_written += flush_to_db(points_buffer)

            duration = time.time() - start_time
            logger.info(f"任務完成! 總計寫入 {total_points_written} 筆 (耗時 {duration:.2f} 秒)")
        
        except Exception as e:
            logger.exception("Worker Thread 發生未預期錯誤，正在重試...")
            time.sleep(1)
        finally:
            job_queue.task_done()

# 🌟 啟動多線程工人池
logger.info(f"啟動 {MAX_WORKERS} 個背景處理線程 (Multi-Worker Pool)...")
for i in range(MAX_WORKERS):
    t = threading.Thread(target=process_data_worker, args=(i,), name=str(i), daemon=True)
    t.start()

@app.route('/collect', methods=['POST'])
@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
def collect():
    try:
        healthkit_data = request.get_json(force=True, silent=True)
        if not healthkit_data: return jsonify({"status": "error", "message": "Empty JSON"}), 400
        job_queue.put(healthkit_data)
        logger.info(f"HTTP: 收到請求並加入佇列 (目前佇列長度: {job_queue.qsize()})")
        return jsonify({"status": "processing", "message": "Queued"}), 202
    except Exception as e:
        logger.exception("Server Error")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health(): return "OK", 200

if __name__ == "__main__":
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"🚀 Apple Health Ingester v10.0 (多線程全開極速版)")
    app.run(host='0.0.0.0', port=5354, debug=False)
