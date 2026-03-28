#!/usr/bin/env python3
"""
Apple Health 資料除錯接收器
用途：接收手機導出的原始 JSON 資料，完整輸出結構以便分析
"""

import os
import sys
import json
import socket
from datetime import datetime
from flask import request, Flask, jsonify

app = Flask(__name__)

# 儲存原始資料的目錄
DATA_DIR = "/app/raw_data"
os.makedirs(DATA_DIR, exist_ok=True)

@app.route('/collect', methods=['POST'])
@app.route('/api/healthautoexport/v1/influxdb/ingest', methods=['POST'])
def collect():
    try:
        # 獲取原始 JSON
        raw_data = request.get_json(force=True, silent=True)
        
        if not raw_data:
            return jsonify({"status": "error", "message": "Empty JSON"}), 400
        
        # 生成檔案名稱（使用時間戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{DATA_DIR}/raw_{timestamp}.json"
        
        # 儲存原始 JSON 到檔案
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"📥 收到資料！已儲存至: {filename}")
        print(f"{'='*60}\n")
        
        # 分析結構
        print("📊 資料結構分析：")
        print(f"  - 頂層 keys: {list(raw_data.keys())}")
        
        if 'data' in raw_data:
            data = raw_data['data']
            print(f"\n📁 data 區塊：")
            print(f"  - data keys: {list(data.keys())}")
            
            if 'metrics' in data:
                metrics = data['metrics']
                print(f"\n📈 metrics 數量: {len(metrics)}")
                for i, m in enumerate(metrics[:5]):  # 只顯示前 5 個
                    name = m.get('name', 'unknown')
                    unit = m.get('unit', '')
                    count = len(m.get('data', []))
                    print(f"  [{i}] {name} ({unit}): {count} 筆")
                if len(metrics) > 5:
                    print(f"  ... 還有 {len(metrics) - 5} 個 metrics")
            
            if 'workouts' in data:
                workouts = data['workouts']
                print(f"\n🏃 workouts 數量: {len(workouts)}")
                for i, w in enumerate(workouts[:3]):  # 只顯示前 3 個
                    name = w.get('name', 'unknown')
                    start = w.get('start', w.get('startDate', 'unknown'))
                    print(f"  [{i}] {name} - {start}")
                    # 顯示這個 workout 有哪些 keys
                    print(f"       keys: {list(w.keys())}")
                if len(workouts) > 3:
                    print(f"  ... 還有 {len(workouts) - 3} 個 workouts")
        
        # 顯示第一個 metric 的完整結構（作為範例）
        if 'data' in raw_data and 'metrics' in raw_data['data'] and len(raw_data['data']['metrics']) > 0:
            print(f"\n📝 第一個 metric 完整結構（範例）：")
            first_metric = raw_data['data']['metrics'][0]
            print(json.dumps(first_metric, ensure_ascii=False, indent=2)[:500])
            if len(json.dumps(first_metric)) > 500:
                print("... (截斷)")
        
        # 顯示第一個 workout 的完整結構（作為範例）
        if 'data' in raw_data and 'workouts' in raw_data['data'] and len(raw_data['data']['workouts']) > 0:
            print(f"\n📝 第一個 workout 完整結構（範例）：")
            first_workout = raw_data['data']['workouts'][0]
            workout_json = json.dumps(first_workout, ensure_ascii=False, indent=2)
            print(workout_json[:1000])
            if len(workout_json) > 1000:
                print("... (截斷)")
        
        print(f"\n{'='*60}\n")
        
        return jsonify({
            "status": "received",
            "message": "Data saved for debugging",
            "filename": filename,
            "summary": {
                "metrics_count": len(raw_data.get('data', {}).get('metrics', [])),
                "workouts_count": len(raw_data.get('data', {}).get('workouts', []))
            }
        }), 200
        
    except Exception as e:
        print(f"❌ 錯誤: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return "Debug Receiver OK", 200

@app.route('/list', methods=['GET'])
def list_files():
    """列出所有已接收的原始資料檔案"""
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json')], reverse=True)
    return jsonify({"files": files, "count": len(files)})

if __name__ == "__main__":
    hostname = socket.gethostname()
    print(f"🔍 Apple Health Debug Receiver")
    print(f"   資料儲存目錄: {DATA_DIR}")
    print(f"   啟動時間: {datetime.now()}")
    app.run(host='0.0.0.0', port=5355, debug=False)
