#!/bin/bash
# 建置並推送 Apple Health InfluxDB 2.x 映像

set -e

IMAGE_NAME="morris0518/apple_health_to_influxdb"
IMAGE_TAG="v3.1-influxdb2"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

echo "========================================="
echo "建置 Apple Health InfluxDB 2.x 映像"
echo "========================================="
echo ""

# 建置映像
echo "🔨 建置 Docker 映像..."
docker build -t "$FULL_IMAGE" .

# 標記為 latest
docker tag "$FULL_IMAGE" "${IMAGE_NAME}:latest"

echo ""
echo "✅ 建置完成！"
echo ""
echo "映像標籤："
echo "  - $FULL_IMAGE"
echo "  - ${IMAGE_NAME}:latest"
echo ""
echo "========================================="
echo "推送至 Docker Hub"
echo "========================================="
echo ""

# 推送映像
echo "📤 推送 $FULL_IMAGE..."
docker push "$FULL_IMAGE"

echo "📤 推送 ${IMAGE_NAME}:latest..."
docker push "${IMAGE_NAME}:latest"

echo ""
echo "✅ 推送完成！"
echo ""
echo "========================================="
echo "下一步：更新 K8s Deployment"
echo "========================================="
echo ""
echo "執行以下命令來更新 K8s："
echo "  kubectl set image deployment/morris-apple-health container-0=$FULL_IMAGE -n health"
echo ""
