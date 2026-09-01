import json
import sys
import time
from pathlib import Path
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


_AVRO_DESERIALIZER = None


def _get_avro_deserializer():
    global _AVRO_DESERIALIZER
    if _AVRO_DESERIALIZER is None:
        schema_path = Path(__file__).parent / "schemas" / "forum_url.json"
        schema_str = schema_path.read_text(encoding="utf-8")
        schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8082"})
        _AVRO_DESERIALIZER = AvroDeserializer(
            schema_registry_client,
            schema_str,
            from_dict=lambda obj, ctx: obj,
        )
    return _AVRO_DESERIALIZER


def _safe_decode_value(value_bytes: bytes, topic: str):
    if value_bytes is None:
        return None

    if len(value_bytes) > 5 and value_bytes[0] == 0x00:
        schema_id = int.from_bytes(value_bytes[1:5], byteorder="big")
        try:
            value = _get_avro_deserializer()(
                value_bytes,
                SerializationContext(topic, MessageField.VALUE),
            )
            if isinstance(value, dict):
                value["_schema_id"] = schema_id
            return value
        except Exception as avro_err:
            sys.stderr.write(
                f"[warn] Deserialize Avro (schema_id={schema_id}) lỗi: "
                f"{type(avro_err).__name__}: {avro_err}. Thử fallback JSON/text...\n"
            )
            sys.stderr.flush()
            payload = value_bytes[5:]
    else:
        payload = value_bytes

    try:
        return json.loads(payload.decode("utf-8"))
    except Exception:
        raise UnicodeDecodeError


# Step 1: Get all message from kafka
conf = {
    'bootstrap.servers': 'localhost:9094',

    # Dùng group id tạm thời để tránh nhầm lẫn với group cũ
    'group.id': 'test-consumer-forum-debug-2',

    # Đọc từ đâu khi group này lần đầu tiên xuất hiện ('earliest' hoặc 'latest')
    'auto.offset.reset': 'earliest',

    # Tự động commit offset sau khi đọc (mặc định là True)
    # Tắt đi để sử dụng Manual Commit, đảm bảo an toàn dữ liệu
    'enable.auto.commit': False
}


# 2. Khởi tạo đối tượng Consumer
consumer = Consumer(conf)

from confluent_kafka import TopicPartition

def _on_assign(consumer_obj, partitions):
    """Rebalance ASSIGN listener: seek về offset 0 rõ ràng, không dùng committed offset cũ."""
    if not partitions:
        return
    seek_partitions = [
        TopicPartition(p.topic, p.partition, 0)
        for p in partitions
    ]
    consumer_obj.assign(seek_partitions)
    sys.stderr.write(
        f"[rebalance] ASSIGNED + seek to 0: "
        f"{[(p.topic, p.partition) for p in seek_partitions]}\n"
    )
    sys.stderr.flush()


def _on_revoke(consumer_obj, partitions):
    """Rebalance REVOKE listener: unassign partitions và log."""
    consumer_obj.unassign()
    if partitions:
        sys.stderr.write(
            f"[rebalance] REVOKED: "
            f"{[(p.topic, p.partition) for p in partitions]}\n"
        )
        sys.stderr.flush()


topics = ['test_topic']  # Thay bằng tên topic của bạn
consumer.subscribe(
    topics,
    on_assign=_on_assign,
    on_revoke=_on_revoke,
)

print(f"Đang bắt đầu listen từ topic: {topics}... Nhấn Ctrl+C để dừng.")

try:
    empty_polls = 0
    last_heartbeat_ts = time.time()
    HEARTBEAT_INTERVAL_S = 30  # in ra 1 dòng heartbeat nếu idle quá 30 giây

    while True:
        # poll(timeout): Đợi tối đa 1.0 giây để nhận tin nhắn mới
        msg = consumer.poll(timeout=1.0)

        # Nếu không có tin nhắn nào trong khoảng timeout
        if msg is None:
            empty_polls += 1
            now = time.time()
            if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL_S:
                sys.stderr.write(
                    f"[heartbeat] idle {empty_polls} polls | chờ message mới...\n"
                )
                sys.stderr.flush()
                last_heartbeat_ts = now
            continue
        # Reset heartbeat khi có bất kỳ poll nào trả về kết quả (msg hoặc error)
        empty_polls = 0
        last_heartbeat_ts = time.time()
        
        # Nếu có lỗi xảy ra trong quá trình poll
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Đã đọc hết tin nhắn hiện tại của partition (báo hiệu kết thúc tạm thời)
                sys.stderr.write(f"Đạt đến cuối partition: {msg.topic()} [{msg.partition()}] offset {msg.offset()}\n")
            else:
                # Lỗi nghiêm trọng khác
                raise KafkaException(msg.error())
        else:
            # 5. Xử lý tin nhắn thành công
            # Dữ liệu nhận về từ Kafka luôn ở dạng Bytes, bạn cần decode sang string
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            
            key_bytes = msg.key()
            value_bytes = msg.value()

            # Decode key (StringSerializer UTF-8 từ producer)
            try:
                key = key_bytes.decode('utf-8') if key_bytes else None
            except UnicodeDecodeError:
                key = key_bytes.decode('latin-1') if key_bytes else None

            # Decode value an toàn
            try:
                value = _safe_decode_value(value_bytes, topic)
            except Exception as e:
                value = f"<decode error: {e}>"

            print("---")
            print(f"Topic: {topic} | Partition: {partition} | Offset: {offset}")
            print("---")
            print(f"Topic: {topic} | Partition: {partition} | Offset: {offset}")
            print(f"Key: {key}")
            print(f"Value raw ({len(value_bytes) if value_bytes else 0} bytes): {value_bytes!r}")
            print("Value decoded (JSON):")
            if isinstance(value, (dict, list)):
                print(json.dumps(value, ensure_ascii=False, indent=2))
            else:
                print(repr(value))
            # 6. Commit thủ công sau khi ĐÃ XỬ LÝ XONG bản tin
            # asynchronous=False nghĩa là nó sẽ block cho tới khi commit thành công
            consumer.commit(asynchronous=False)
            # print(f"Đã commit thủ công offset: {offset}")

except KeyboardInterrupt:
    # Bắt sự kiện khi người dùng nhấn Ctrl+C để tắt ứng dụng sạch sẽ
    print("\nĐang dừng consumer...")