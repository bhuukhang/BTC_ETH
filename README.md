# XÂY DỰNG QUY TRÌNH TỰ ĐỘNG HÓA (END TO END) PHỤC VỤ PHÂN TÍCH VÀ TRỰC QUAN HÓA DỮ LIỆU BTC VÀ ETH TRÊN POWER BI 

## Tổng quan
Đồ án tập trung xây dựng một quy trình xử lý dữ liệu lớn hoàn chỉnh cho bộ 
dữ liệu BTC & ETH Hourly Market Metrics (2017–2026), với mục tiêu tổ chức 
dữ liệu từ đầu vào đến đầu ra theo hướng tự động hóa. Toàn bộ hệ thống được 
triển khai trên nhiều công nghệ khác nhau, bao gồm Docker, HDFS, Hive, 
PostgreSQL, Power BI và Airflow, giúp hình thành một pipeline dữ liệu rõ ràng, 
đồng bộ và có khả năng vận hành ổn định.

## Quy trình xử lý
CSV → HDFS → Hive → PostgreSQL → Power BI

## Công cụ sử dụng
- Python
- Pandas
- Apache Airflow
- Hadoop HDFS
- Apache Hive
- PostgreSQL
- Power BI
- Docker
- SQL

## Xây dựng quy trình

![Quy trình dự án](images/Screen<img width="767" height="400" alt="Screenshot 2026-04-28 113811" src="https://github.com/user-attachments/assets/f2b853a3-492f-41e7-806e-2ab71b123aa1" />
shot 2026-04-28 113811.png)

## Nội dung phân tích
- Hiệu suất giá  
- Xu hướng thanh khoản
- Chỉ số rủi ro thị trường

## Kết quả
Hoàn thành hệ thống tự động cập nhật dữ liệu và dashboard hỗ trợ phân tích BTC, ETH.
