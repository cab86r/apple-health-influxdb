FROM python:3.11-slim

WORKDIR /app

# 安裝相依套件
RUN pip install --no-cache-dir \
    influxdb-client \
    gunicorn \
    flask \
    geolib

# 複製應用程式
COPY app.py .

# 公開連接埠
EXPOSE 5354

# 啟動命令
CMD ["gunicorn", "--bind", "0.0.0.0:5354", "--workers", "2", "--threads", "4", "app:app"]
