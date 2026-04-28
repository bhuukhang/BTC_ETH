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
<img width="767" height="400" alt="Screenshot 2026-04-28 113811" src="https://github.com/user-attachments/assets/f2b853a3-492f-41e7-806e-2ab71b123aa1" />

- Docker Host & docker-compose.yml: Toàn bộ hệ thống (trừ Power BI) được gói gọn trong các Docker container. File docker-compose.yml làm nhiệm vụ khai báo và quản lý tất cả các dịch vụ này cùng lúc.

- Mạng Docker (Docker Network): Tạo một mạng nội bộ cho phép các container (như Hive, Hadoop, Postgres, Airflow) giao tiếp an toàn với nhau (service-to-service).

- Mounts Volume (Ánh xạ dữ liệu): Các thư mục trên máy tính thật (Local Host Machine) được liên kết vào trong container. Điều này giúp code (như các file DAG của Airflow trong ./dags), file log (./logs) và dữ liệu thực tế (./data) không bị mất đi khi container tắt.

- Lưu trữ phân tán (Hadoop HDFS): Đóng vai trò là Data Lake.

* NameNode: Quản lý "mục lục" của dữ liệu.

* DataNode: Nơi trực tiếp lưu trữ các khối dữ liệu thực tế. Nó sẽ chứa cả dữ liệu thô (Raw) chưa qua xử lý.

- Xử lý dữ liệu (Hive Server): Đóng vai trò là Data Warehouse. Hive sẽ lấy dữ liệu từ HDFS, sử dụng ngôn ngữ truy vấn HiveQL để làm sạch, biến đổi (Clean/Transform) dữ liệu thô thành dữ liệu có cấu trúc.

- Lưu trữ quan hệ (PostgreSQL): Sau khi Hive xử lý xong, dữ liệu sạch (Clean Data) sẽ được nạp (load) vào PostgreSQL. Khác với HDFS dùng để lưu trữ file lớn, Postgres là cơ sở dữ liệu quan hệ, tối ưu hóa cho việc truy xuất nhanh và phục vụ cho các ứng dụng đầu cuối.

- Trực quan hóa (Power BI): Đóng vai trò là BI Tool. Power BI nằm ngoài môi trường Docker, nó kết nối trực tiếp vào PostgreSQL qua cổng mạng để lấy dữ liệu sạch, từ đó xây dựng các dashboard phục vụ cho việc ra quyết định kinh doanh.

- Apache Airflow: Đây là "nhạc trưởng" của toàn bộ hệ thống. Thay vì bạn phải chạy tay từng bước, Airflow sẽ dùng các đồ thị có hướng (DAGs) để lên lịch (Scheduler) và tự động hóa quy trình theo thứ tự:

* Kích hoạt Task HDFS (đưa dữ liệu vào).

* Chạy Task Hive (để xử lý và làm sạch dữ liệu).

* Nạp dữ liệu vào PostgreSQL.

## Nội dung phân tích
- Tổng quan
- Hiệu suất thị trường

## Kết quả
<img width="1289" height="724" alt="Screenshot 2026-04-28 112615" src="https://github.com/user-attachments/assets/2ce71088-8ce9-4a11-b810-6525fd06e877" />

<img width="1287" height="724" alt="Screenshot 2026-04-28 112636" src="https://github.com/user-attachments/assets/92b69c08-e9ef-46f4-a9c4-40427b9616cd" />

