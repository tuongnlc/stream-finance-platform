from redis import Redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

class RedisDatabaseConnector:
    _instance: Redis | None = None

    def __new__(cls, *args, **kwargs) -> Redis:
        if cls._instance is None:
            cls._instance = Redis(host=REDIS_HOST, port=REDIS_PORT)
        print(cls._instance)
        
        return cls._instance

connection = RedisDatabaseConnector()
