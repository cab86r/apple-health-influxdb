#!/bin/bash
# 初始化 Git 並推送到 GitHub

cd ~/.openclaw/workspace/apple-health-influxdb-v2

# 初始化 git
git init
git add .
git commit -m "Add Apple Health InfluxDB 2.x ingester"

# 建立 GitHub repository 並推送
# 需要先安裝 gh CLI: brew install gh
gh repo create apple-health-influxdb --public --source=. --push

echo ""
echo "✅ Repository 已建立！"
echo ""
echo "接下來請到 GitHub 設定 Secrets："
echo "1. 前往 https://github.com/morris9601/apple-health-influxdb/settings/secrets/actions"
echo "2. 新增以下 secrets:"
echo "   - DOCKERHUB_USERNAME: morris0518"
echo "   - DOCKERHUB_TOKEN: <你的 Docker Hub Access Token>"
echo ""
echo "3. 設定完成後，Actions 會自動建置並推送映像"
