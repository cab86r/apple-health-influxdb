# Apple Health to InfluxDB 2.x Ingester v3.0

支援 InfluxDB 2.x Token 認證的 Apple Health 資料收集服務。

## ✨ v3.0 新功能

- 🚀 **多線程處理** - ThreadPoolExecutor 處理大量並發請求
- ⚡ **非同步批次寫入** - WriteOptions 優化大數據量寫入效能
- 📱 **支援 Apple Shortcuts** - 可接收捷徑 (Shortcuts) 扁平化格式
- 🔀 **統一入口** - `/collect` 和 `/ingest` 使用同一處理函數
- ✅ **HTTP 202** - 立即回應，背景處理

## 功能

- ✅ 支援 InfluxDB 2.x Token 認證
- ✅ 接收 Health Auto Export JSON 格式
- ✅ 接收 Apple Shortcuts 扁平化格式
- ✅ 處理健康指標（步數、心率、睡眠等）
- ✅ 處理運動資料
- ✅ 支援多設備（target 參數）
- ✅ 非同步批次寫入優化

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `INFLUX_HOST` | InfluxDB 主機 | `localhost` |
| `INFLUX_PORT` | InfluxDB 連接埠 | `8086` |
| `INFLUX_TOKEN` | API Token | (必填) |
| `INFLUX_ORG` | 組織名稱 | `unifi` |
| `INFLUX_DB` | Bucket 名稱 | `apple-health-v2` |
| `DATAPOINTS_CHUNK` | 批次寫入大小 | `5000` |
| `MAX_WORKERS` | 背景線程數量 | `4` |

## 支援的資料格式

### 格式 1: Health Auto Export (標準格式)

```json
{
  "data": {
    "metrics": [
      {
        "name": "heart_rate",
        "unit": "bpm",
        "data": [
          {"date": "2026-03-27T10:00:00Z", "qty": 72}
        ]
      }
    ],
    "workouts": [
      {
        "name": "戶外跑步",
        "startDate": "2026-03-27T06:00:00Z",
        "duration": 1800,
        "distance": 5000
      }
    ]
  }
}
```

### 格式 2: Apple Shortcuts (扁平化格式)

```json
[
  {
    "name": "heart_rate",
    "unit": "bpm",
    "qty": 72,
    "date": "2026-03-27T10:00:00Z"
  },
  {
    "metric": "steps",
    "value": 1000,
    "time": "2026-03-27T10:00:00Z"
  }
]
```

## 建置與部署

### 1. 建置 Docker 映像

```bash
cd /tmp/apple-health-influxdb
docker build -t morris0518/apple_health_to_influxdb:v3-influxdb2 .
docker push morris0518/apple_health_to_influxdb:v3-influxdb2
```

### 2. 部署到 K8s

```bash
kubectl set image deployment/morris-apple-health \
  container-0=morris0518/apple_health_to_influxdb:v3-influxdb2 \
  -n health
```

### 3. 驗證部署

```bash
kubectl get pods -n health
kubectl logs -n health deployment/morris-apple-health --tail=50
curl http://192.168.1.35:5354/health
```

## Health Auto Export 設定

在 iOS 的 Health Auto Export app 中：

1. 建立新的 Automation
2. 選擇 REST API
3. URL: `http://your-server:5354/collect`
4. 格式: JSON

## 端點

| 端點 | 方法 | 說明 |
|------|------|------|
| `/` | GET | 服務資訊 |
| `/health` | GET | 健康檢查 |
| `/collect` | POST | 接收資料（相容舊版） |
| `/api/healthautoexport/v1/influxdb/ingest` | POST | 接收資料（新版） |

## 疑難排解

### 401 Unauthorized

- 確認 `INFLUX_TOKEN` 正確
- 確認 Token 有寫入 bucket 的權限

### 連線失敗

- 確認 InfluxDB 2.x 服務正在運行
- 確認 K8s Service DNS 解析正確

### 無資料

- 檢查日誌中的錯誤訊息
- 確認 Health Auto Export 設定正確

## 版本歷史

- **v3.0.0** (2026-03-27): 多線程處理、非同步批次寫入、支援 Apple Shortcuts
- **v2.1.0** (2026-03-27): 新增 `/collect` 端點相容舊版
- **v2.0.0** (2026-03-27): 支援 InfluxDB 2.x Token 認證
- **v1.x**: 舊版本（僅支援 InfluxDB 1.8）
