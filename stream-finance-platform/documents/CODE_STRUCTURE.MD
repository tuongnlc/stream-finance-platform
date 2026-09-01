# Code Structure

## 1. Muc tieu

Tai lieu nay mo ta kien truc co ban cua project theo huong:

- Don gian, de doc, de mo rong
- Tach ro `application`, `business logic`, `infrastructure`
- Phu hop voi nhieu ung dung nhu:
  - `real-time-crawl`
  - `real-time-processing`
- De tai su dung cac thanh phan dung chung nhu:
  - Redis
  - Kafka
  - HTTP client
  - Schema

## 2. Nguyen tac to chuc code

Project duoc chia theo trach nhiem chinh:

- `applications/`
  - Chua entrypoint cua tung ung dung
  - Moi application chi lo chay luong chinh
  - Khong chua logic Redis, Kafka, HTTP o muc thap

- `services/`
  - Chua business logic hoac orchestration logic
  - Dieu phoi cac buoc nghiep vu
  - Goi repository, client, producer, consumer

- `infrastructure/`
  - Chua code giao tiep voi he thong ben ngoai
  - Bao gom:
    - Redis
    - Kafka
    - HTTP client
    - cac external systems khac trong tuong lai

- `schemas/`
  - Chua schema dung chung, vi du Avro schema cho Kafka message

## 3. Structure de xuat

```text
stream-finance-platform/
├── applications/
│   ├── realtime_crawl/
│   │   └── main.py
│   └── realtime_processing/
│       └── main.py
├── services/
│   ├── crawl_service.py
│   └── forum_service.py
├── infrastructure/
│   ├── database/
│   │   └── redis/
│   │       ├── db.py
│   │       └── crawl_state_repository.py
│   ├── messaging/
│   │   └── kafka/
│   │       ├── producer.py
│   │       └── consumer.py
│   └── clients/
│       └── forum_client.py
├── schemas/
│   └── forum_url.json
└── documents/
    └── CODE_STRUCTURE.MD
```

## 4. Vai tro tung thu muc

### 4.1 `applications/`

Day la noi chay tung ung dung cu the.

Vi du:

- `applications/realtime_crawl/main.py`
  - doc trang thai crawl hien tai
  - goi service de lay page moi
  - gui message vao Kafka
  - cap nhat Redis

- `applications/realtime_processing/main.py`
  - consume message tu Kafka
  - goi service de xu ly du lieu forum
  - chuan bi cho cac buoc xu ly tiep theo

Quy tac:

- File trong `applications/` nen ngan
- Chi dong vai tro entrypoint
- Khong nen chua qua nhieu logic nghiep vu

### 4.2 `services/`

Day la noi chua business logic hoac orchestration logic.

Vi du:

- `crawl_service.py`
  - xac dinh page moi can crawl
  - build Kafka message
  - dieu phoi luong crawl

- `forum_service.py`
  - xu ly du lieu bai viet lay tu forum
  - chuan hoa output
  - phuc vu cho downstream processing

Luu y:

- `services/` trong project nay gan voi `use_case` layer
- Neu sau nay muon dat ten ro hon, co the doi `services/` thanh `use_cases/`

### 4.3 `infrastructure/`

Day la noi chua code giao tiep voi he thong ben ngoai.

#### `infrastructure/database/redis/`

- `db.py`
  - khoi tao Redis connection dung chung

- `crawl_state_repository.py`
  - doc/ghi trang thai crawl vao Redis
  - vi du:
    - lay `last_page`
    - cap nhat `last_page`

#### `infrastructure/messaging/kafka/`

- `producer.py`
  - gui message vao Kafka

- `consumer.py`
  - subscribe topic
  - poll message
  - deserialize message
  - commit offset

#### `infrastructure/clients/`

- `forum_client.py`
  - goi HTTP toi forum
  - lay raw HTML
  - lay `last_page` tu redirect URL
  - parse du lieu tho tu trang forum

Luu y:

- `forum_client.py` nam trong `infrastructure` vi forum la external system
- `infrastructure` khong chi danh cho database
- Bat ky thanh phan nao giao tiep voi ben ngoai deu thuoc `infrastructure`

### 4.4 `schemas/`

Chua schema dung chung cho message hoac du lieu trao doi giua cac service.

Vi du:

- `forum_url.json`
  - schema Avro cho message gui vao Kafka

## 5. Luong du lieu co ban

### 5.1 Realtime Crawl

Luong `realtime_crawl`:

1. Doc `last_page` hien tai tu Redis
2. Goi forum de xac dinh `last_page` moi nhat
3. So sanh page cu va page moi
4. Tao message crawl cho tung page moi
5. Gui message vao Kafka
6. Neu gui thanh cong, cap nhat lai `last_page` trong Redis

### 5.2 Realtime Processing

Luong `realtime_processing`:

1. Consume message tu Kafka
2. Deserialize message
3. Lay URL hoac payload can xu ly
4. Goi forum de lay noi dung bai viet
5. Parse danh sach post
6. Tra du lieu cho buoc xu ly tiep theo

## 6. Mapping tu code hien tai sang structure moi

### 6.1 File `check_last_page_and_compare.py`

Cac phan hien tai nen tach nhu sau:

- `determine_last_page(...)`
  - chuyen sang `infrastructure/clients/forum_client.py`

- `query_and_compare_last_page(...)`
  - chuyen sang `infrastructure/database/redis/crawl_state_repository.py`

- `update_last_page(...)`
  - chuyen sang `infrastructure/database/redis/crawl_state_repository.py`

- `build_kafka_key(...)`
  - chuyen sang `services/crawl_service.py`

- `_build_message_value(...)`
  - chuyen sang `services/crawl_service.py`

- `_send_message_to_kafka(...)`
  - chuyen sang `infrastructure/messaging/kafka/producer.py`

- `main()`
  - chuyen sang `applications/realtime_crawl/main.py`

### 6.2 File `get_forum_content.py`

Cac phan hien tai nen tach nhu sau:

- `fetch_real_html(...)`
  - phan goi HTTP va parse tho chuyen sang `infrastructure/clients/forum_client.py`

- logic xu ly danh sach post
  - chuyen sang `services/forum_service.py`

- `main()`
  - co the dung trong `applications/realtime_processing/main.py`

### 6.3 File `get_mesage_from_kafka.py`

Cac phan hien tai nen tach nhu sau:

- `_get_avro_deserializer()`
  - chuyen sang `infrastructure/messaging/kafka/consumer.py`

- `_safe_decode_value(...)`
  - chuyen sang `infrastructure/messaging/kafka/consumer.py`

- cau hinh consumer, subscribe, poll, commit
  - chuyen sang `infrastructure/messaging/kafka/consumer.py`

- logic xu ly business sau khi nhan message
  - chuyen sang `services/forum_service.py`

- `main()`
  - chuyen sang `applications/realtime_processing/main.py`

## 7. Repository co dung o dau

Trong kien truc nay:

- Redis co the dung `repository`
- Kafka khong can dung `repository`

Ly do:

- Redis dang dong vai tro luu trang thai nghiep vu nhu `last_page`
- Kafka la message broker, phu hop voi `producer` va `consumer`

Vi du:

- `crawl_state_repository.py`
  - `get_last_page(source, category)`
  - `update_last_page(source, category, page)`

## 8. Quy uoc thiet ke

### 8.1 Application layer

- Chi goi service
- Khong khoi tao truc tiep Redis, Kafka, HTTP client neu co the tach ra
- Khong chua qua nhieu business logic

### 8.2 Service layer

- Chua luong nghiep vu
- Goi infrastructure de lay/gui du lieu
- Khong nen chua qua nhieu chi tiet ky thuat nhu config, serializer setup, Redis connection setup

### 8.3 Infrastructure layer

- Chua code tich hop voi external systems
- Co the thay doi chi tiet implementation ma it anh huong toi business logic

## 9. Dinh huong mo rong

Khi project lon hon, co the mo rong them:

- `use_cases/`
  - neu muon tach ro hon giua service va application use case

- `domain/`
  - neu bat dau co entity, enum, value object, business rule phuc tap

- `config/` hoac `settings.py`
  - gom cau hinh Redis, Kafka, Schema Registry, HTTP timeout

Hien tai chua bat buoc phai them cac layer nay de tranh over-engineering.

## 10. Ket luan

Kien truc co ban cua project nen di theo huong:

- `applications/` de chay tung app
- `services/` de chua logic nghiep vu
- `infrastructure/` de giao tiep voi Redis, Kafka, HTTP
- `schemas/` de chua schema dung chung

Cach to chuc nay phu hop voi giai doan hien tai vi:

- du ro rang
- du don gian
- de refactor tu cac script hien co
- de mo rong cho nhieu application sau nay
