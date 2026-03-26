"""
Apple Health to InfluxDB 2.x Ingester
支援 Health Auto Export JSON 格式
"""

import os
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# InfluxDB 2.x 設定
INFLUX_URL = f"http://{os.getenv('INFLUX_HOST', 'localhost')}:{os.getenv('INFLUX_PORT', '8086')}"
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'unifi')
INFLUX_BUCKET = os.getenv('INFLUX_DB', 'apple-health-v2')
DATAPOINTS_CHUNK = int(os.getenv('DATAPOINTS_CHUNK', '10000'))

# 初始化 InfluxDB 用戶端
def get_influx_client():
    """建立 InfluxDB 2.x 用戶端連線"""
    try:
        client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        )
        # 測試連線
        health = client.health()
        if health.status == "pass":
            logger.info(f"✅ InfluxDB 2.x 連線成功: {INFLUX_URL}")
            return client
        else:
            logger.error(f"❌ InfluxDB 健康檢查失敗: {health}")
            return None
    except Exception as e:
        logger.error(f"❌ InfluxDB 連線失敗: {e}")
        return None

def write_metrics(data, target_name=None):
    """寫入健康資料到 InfluxDB"""
    client = get_influx_client()
    if not client:
        return False, "無法連線 InfluxDB"
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    
    try:
        # 處理健康指標
        if 'data' in data and 'metrics' in data['data']:
            for metric in data['data']['metrics']:
                metric_name = metric.get('name', 'unknown')
                unit = metric.get('unit', '')
                
                # 處理每個資料點
                for datapoint in metric.get('data', []):
                    date = datapoint.get('date')
                    qty = datapoint.get('qty')
                    
                    if date and qty is not None:
                        # 建立 Point
                        measurement = f"{metric_name}_{unit}" if unit else metric_name
                        point = Point(measurement) \
                            .tag("source", "apple_health") \
                            .field("qty", float(qty)) \
                            .time(date, WritePrecision.NS)
                        
                        if target_name:
                            point = point.tag("target", target_name)
                        
                        points.append(point)
        
        # 處理運動資料
        if 'data' in data and 'workouts' in data['data']:
            for workout in data['data']['workouts']:
                workout_name = workout.get('name', 'unknown')
                start_date = workout.get('startDate')
                
                # 運動摘要
                point = Point("workout") \
                    .tag("source", "apple_health") \
                    .tag("workout_name", workout_name) \
                    .time(start_date, WritePrecision.NS)
                
                if target_name:
                    point = point.tag("target", target_name)
                
                # 加入運動統計
                for key, value in workout.items():
                    if key not in ['name', 'startDate', 'endDate'] and value is not None:
                        if isinstance(value, (int, float)):
                            point = point.field(key, value)
                
                points.append(point)
                
                # 處理運動中的時間序列資料
                if 'heartRateData' in workout:
                    for hr_datapoint in workout['heartRateData']:
                        point = Point("heart_rate_data_bpm") \
                            .tag("source", "apple_health") \
                            .tag("workout_name", workout_name) \
                            .field("qty", float(hr_datapoint.get('qty', 0))) \
                            .time(hr_datapoint.get('date'), WritePrecision.NS)
                        
                        if target_name:
                            point = point.tag("target", target_name)
                        
                        points.append(point)
        
        # 批次寫入
        if points:
            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=points
            )
            logger.info(f"✅ 成功寫入 {len(points)} 個資料點到 {INFLUX_BUCKET}")
        
        client.close()
        return True, f"寫入 {len(points)} 個資料點"
        
    except Exception as e:
        logger.error(f"❌ 寫入資料失敗: {e}")
        client.close()
        return False, str(e)

@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
def ingest():
    """接收 Health Auto Export 資料"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "無效的 JSON 資料"}), 400
        
        # 取得 target 參數（可選）
        target_name = request.args.get('target', None)
        
        # 寫入資料
        success, message = write_metrics(data, target_name)
        
        if success:
            return jsonify({
                "status": "success",
                "message": message,
                "timestamp": datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": message
            }), 500
            
    except Exception as e:
        logger.error(f"❌ 處理請求失敗: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """健康檢查端點"""
    client = get_influx_client()
    if client:
        client.close()
        return jsonify({"status": "healthy"}), 200
    else:
        return jsonify({"status": "unhealthy"}), 503

@app.route('/', methods=['GET'])
def index():
    """首頁"""
    return jsonify({
        "service": "Apple Health to InfluxDB 2.x Ingester",
        "version": "2.0.0",
        "endpoints": {
            "ingest": "/api/healthautoexport/v1/influxdb/ingest",
            "health": "/health"
        }
    }), 200

if __name__ == '__main__':
    logger.info(f"🚀 啟動 Apple Health Ingester v2.0")
    logger.info(f"📊 InfluxDB: {INFLUX_URL}")
    logger.info(f"📦 Bucket: {INFLUX_BUCKET}")
    logger.info(f"🏢 Org: {INFLUX_ORG}")
    app.run(host='0.0.0.0', port=5354, debug=False)
