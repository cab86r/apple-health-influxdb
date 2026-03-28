# Apple Health to InfluxDB 2.x Ingester

支援 InfluxDB 2.x Token 認證的 Apple Health 數據收集服務。

## 功能

- ✅ 支援 InfluxDB 2.x Token 認證
- ✅ 接收 Health Auto Export JSON 格式
- ✅ 處理健康指標（步數、心率、睡眠等）
- ✅ 處理運動數據（GPS 軌跡、心率流、activeEnergy 等）
- ✅ **萬用陣列拆解引擎** - 自動處理每分鐘數據
- ✅ **多線程並行寫入** - 16 Workers 極速處理
- ✅ **失敗備份** - 寫入失敗自動存檔
- ✅ 支援多設備（target 參數）

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `INFLUX_HOST` | InfluxDB 主機 | `localhost` |
| `INFLUX_PORT` | InfluxDB 端口 | `8086` |
| `INFLUX_DB` | Bucket 名稱 | `apple-health-v2` |
| `INFLUX_ORG` | 組織名稱 | `unifi` |
| `INFLUX_TOKEN` | API Token | (必填) |
| `DATAPOINTS_CHUNK` | 批次寫入大小 | `2000` |
| `MAX_WORKERS` | 並行 Worker 數 | `16` |

## 建置與部署

### 1. 建置 Docker 映像

```bash
cd ~/.openclaw/workspace/apple-health-influxdb-v2
chmod +x build-and-push.sh
./build-and-push.sh
```

或使用 GitHub Actions：
```bash
gh workflow run docker-publish.yml --repo morris0518/apple-health-influxdb
```

### 2. 部署到 K8s

```bash
# 更新映像版本
kubectl set image deployment/morris-apple-health \
  container-0=morris0518/apple_health_to_influxdb:v10.0 \
  -n health

# 重啟 deployment
kubectl rollout restart deployment/morris-apple-health -n health
```

### 3. 驗證部署

```bash
# 檢查 Pod 狀態
kubectl get pods -n health -l app=apple-health-ingester

# 查看日誌
kubectl logs -n health deployment/morris-apple-health --tail=100 -f

# 測試健康檢查
kubectl port-forward -n health deployment/morris-apple-health 5354:5354 &
curl http://localhost:5354/health
```

## Health Auto Export 設定

在 iOS 的 Health Auto Export app 中：

1. 建立新的 Automation
2. 選擇 REST API
3. URL: `http://your-server:5354/api/healthautoexport/v1/influxdb/ingest`
4. 格式: JSON
5. 可選: 添加 `?target=NAME` 參數來識別數據來源

## 端點

| 端點 | 方法 | 說明 |
|------|------|------|
| `/` | GET | 服務資訊 |
| `/health` | GET | 健康檢查 |
| `/collect` | POST | 接收數據 |
| `/api/healthautoexport/v1/influxdb/ingest` | POST | 接收數據（標準端點）|

## Measurements

| Measurement | 說明 |
|-------------|------|
| `{metric}_{unit}` | 健康指標（如 `stepCount_count`）|
| `workout` | 運動總結 |
| `workout_route` | GPS 軌跡 |
| `workout_heart_rate` | 運動心率流 |
| `workout_activeEnergy` | 每分鐘卡路里 |
| `workout_distance` | 每分鐘距離 |
| `workout_*` | 其他高頻數據 |

## 疑難排解

### 401 Unauthorized

- 確認 `INFLUX_TOKEN` 正確
- 確認 Token 有寫入 `apple-health-v2` bucket 的權限

### 連接失敗

- 確認 InfluxDB 2.x 服務正在運行
- 確認 K8s Service DNS 解析正確
- 測試: `kubectl exec -n health deployment/morris-apple-health -- curl http://influxdb2.unifi-monitoring.svc.cluster.local:8086/health`

### 無數據

- 檢查日誌中的錯誤訊息
- 確認 Health Auto Export 設定正確
- 確認 bucket 和 org 名稱匹配

### 寫入失敗

- 檢查日誌中的 `failed_dump_W*.txt` 檔案
- 這些檔案包含寫入失敗的原始資料
- 可手動重新匯入

## 版本歷史

- **v10.0** (2026-03-28): 多線程全開極速版 - 16 Workers 並行、失敗備份、佇列長度顯示
- **v9.0** (2026-03-28): 萬用陣列拆解引擎 - 自動處理 activeEnergy, distance 等每分鐘數據
- **v3.1-influxdb2** (2026-03-28): 修復 Workout 支援
- **v3-influxdb2** (2026-03-28): 多線程非同步處理
- **v2-influxdb2** (2026-03-27): 支援 InfluxDB 2.x Token 認證
