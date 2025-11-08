# Bắt đầu từ image Spark mà bạn đang dùng
FROM jupyter/pyspark-notebook:latest

# Chuyển sang user 'root' để cài đặt
USER root

# Cài đặt numpy và các thư viện data science khác nếu cần
RUN pip install --no-cache-dir elasticsearch prophet

RUN mkdir -p /usr/local/spark/work && \
    chown -R jovyan:users /usr/local/spark/work

# Chuyển về user mặc định
USER jovyan