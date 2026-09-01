# Step 1: Query last page
import asyncio
import httpx
import redis
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time
import re
import json
from pathlib import Path

# Cấu hình Redis
r = redis.Redis(
    host="localhost", port=6379, db=0, password="redis", decode_responses=True
) 

key_name = "last_page:voz"


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

async def determine_last_page(client: httpx.AsyncClient, url: str) -> int:
    """
        Step 1: Call to url with very large number of page to return to last page
        Step 2: Extract last page number from response url
        :param url: Forum url
        :return: Last page number
    """
    try:
        response = await client.get(url, headers=HEADERS,follow_redirects=True, timeout=10.0)

        last_page = str(response.url).split("page-")[-1]
        return int(last_page)
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def query_and_compare_last_page(chuyen_muc: str) -> int:
    """
        Example:
        await query_and_compare_last_page("clb_ck")
    """
    last_page = r.hget(key_name, chuyen_muc)
    if last_page:
        last_page = int(last_page)
    else:
        raise ValueError(f"Không tìm thấy dữ liệu cho chuyên mục '{chuyen_muc}'")

    return last_page
    
def update_last_page(chuyen_muc: str, last_page: int):
    r.hset(key_name, chuyen_muc, last_page)

def build_kafka_key(url: str) -> str:
    # url mẫu: https://voz.vn/t/....1188208/page-6684
    # Lấy thread id (số ngay trước dấu /page-) và số trang
    m = re.search(r"\.(\d+)/page-(\d+)$", url)
    if not m:
        # Fallback nếu pattern không khớp (tránh crash)
        return url
    page = m.group(2)
    return f"voz:clb_ck:page:{page}"

def _build_message_value(url: str) -> dict:
    return {
        "url": url,
        "crawl_status": "NOT_STARTED",
        "error_message": None, # Có thể là string hoặc None nhờ loại ["null", "string"]
        "updated_at": int(time.time() * 1000), # Epoch time dạng long (mili giây).
    }

def _message_to_dict(message_value: dict, ctx=None) -> dict:
    return message_value

def _create_serializers() -> str:
    sr_config = {'url': 'http://localhost:8082'}
    schema_registry_client = SchemaRegistryClient(sr_config)

    schema_path = Path(__file__).parent / "schemas" / "forum_url.json"
    schema_str = schema_path.read_text(encoding="utf-8")

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        _message_to_dict,
    )
    string_serializer = StringSerializer('utf_8')


    return avro_serializer, string_serializer

def _send_message_to_kafka(url: str):
    err_list = []
    avro_serializer, string_serializer = _create_serializers()

    config = {
        'bootstrap.servers': 'localhost:9094,localhost:9095'
    }

    producer = Producer(config)

    def delivery_report(err, msg):
        if err is not None:
            err_list.append(err)
            print(f"Gửi tin nhắn thất bại: {err}")
        else:
            print(f"Gửi thành công tới topic {msg.topic()} [Partition: {msg.partition()}]")

    # Define message to send
    topic_name = "test_topic" # Tên topic thực tế của bạn
    message_key = build_kafka_key(url)

    message_value = _build_message_value(url)

    try:
        producer.produce(
            topic=topic_name,
            key=string_serializer(message_key, SerializationContext(topic_name, MessageField.KEY)),
            value=avro_serializer(
                message_value,
                SerializationContext(topic_name, MessageField.VALUE)
            ),
            on_delivery=delivery_report
        )
        # Ép producer gửi ngay các tin nhắn còn trong queue
        producer.flush()
    except Exception as e:
        print(f"Lỗi khi gửi: {e}")
    return err_list

def main():
    url = "https://voz.vn/t/clb-chung-khoan-chia-se-kinh-nghiem-dau-tu-chung-khoan-2026-ky-nguyen-vuon-minh.1188208/page-1000000"

    last_page_from_voz = asyncio.run(determine_last_page(httpx.AsyncClient(), url))
    print(last_page_from_voz)

    last_page_from_redis = asyncio.run(query_and_compare_last_page("clb_ck"))
    print(last_page_from_redis)
    
    for i in range(last_page_from_redis, last_page_from_voz+1):
        url_ = f"https://voz.vn/t/clb-chung-khoan-chia-se-kinh-nghiem-dau-tu-chung-khoan-2026-ky-nguyen-vuon-minh.1188208/page-{i}"
        print(url_)
        err_list = _send_message_to_kafka(url_)
    
    if last_page_from_voz > last_page_from_redis and not err_list:
        update_last_page("clb_ck", last_page_from_voz)
        print("No error")

if __name__ == "__main__":
    main()
