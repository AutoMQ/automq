import asyncio
from aiokafka import AIOKafkaProducer
import logging

async def send_one():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    try:
        await producer.start()
        print("Connected!")
        await producer.send_and_wait("iot_telemetry", b"test")
        print("Sent!")
        await producer.stop()
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(send_one())
