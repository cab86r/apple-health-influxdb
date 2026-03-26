# Apple Health to InfluxDB 2.x Ingester

支援 InfluxDB 2.x Token 認證的 Apple Health 數據收集服務。

## 功能

- ✅ 支援 InfluxDB 2.x Token 認證
- ✅ 接收 Health Auto Export JSON 格式
- ✅ 處理健康指標（步數、心率、睡眠等）
- ✅ 處理運動數據
- ✅ 支援多設備（target 參數）

## 環境變數

| 變數 | 說明 | 範例 |
|------|------|------|
| `INFLUX_HOST` | InfluxDB 主機 | `influxdb2.unifi-monitoring.svc.cluster.local` |
| `INFLUX_PORT` | InfluxDB 端口 | `8086` |
| `INFLUX_DB` | Bucket 名稱 | `apple-health-v2` |
| `INFLUX_ORG` | 組織名稱 | `unifi` |
| `INFLUX_TOKEN` | API Token | `your-token-here` |
| `DATAPOINTS_CHUNK` | 批次寫入大小 | `10000` |

## 建置與部署

### 1. 建置 Docker 映像

```bash
cd ~/.openclaw/workspace/apple-health-influxdb-v2
chmod +x build-and-push.sh
./build-and-push.sh
```

### 2. 部署到 K8s

```bash
# 方法 A：使用新的 deployment YAML
kubectl apply -f deployment-v2.yaml

# 方法 B：更新現有 deployment 的映像
kubectl set image deployment/morris-apple-health \
  container-0=morris0518/apple_health_to_influxdb:v2-influxdb2 \
  -n health
```

### 3. 驗證部署

```bash
# 檢查 Pod 狀態
kubectl get pods -n health -l workload.user.cattle.io/workloadselector=apps.deployment-health-morris-apple-health

# 查看日誌
kubectl logs -n health deployment/morris-apple-health --tail=50

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
| `/api/healthautoexport/v1/influxdb/ingest` | POST | 接收數據 |

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

## 版本歷史

- **v2-influxdb2** (2026-03-27): 支援 InfluxDB 2.x Token 認證
- **latest**: 舊版本（僅支援 InfluxDB 1.8）
