FROM python:3.11-slim

WORKDIR /app

# 安裝依賴
RUN pip install --no-cache-dir \
    influxdb-client \
    gunicorn \
    flask

# 複製應用程式
COPY app.py .

# 暴露端口
EXPOSE 5354

# 啟動命令
CMD ["gunicorn", "--bind", "0.0.0.0:5354", "--workers", "2", "--threads", "4", "app:app"]
