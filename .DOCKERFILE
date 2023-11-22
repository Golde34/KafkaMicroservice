# Sử dụng một hình ảnh gốc đã chứa ứng dụng của bạn (lensesio/fast-data-dev:latest)
FROM lensesio/fast-data-dev:latest

# Chỉ định các cổng mạng bạn muốn mở
EXPOSE 2181 3030 8081-8083 9581-9585 9092

# Thiết lập các biến môi trường
ENV ADV_HOST=127.0.0.1
ENV RUN_TESTS=0

# Không cần thiết nếu bạn muốn container chạy ngầm
# CMD ["your-startup-command"]