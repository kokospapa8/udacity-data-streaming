import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer

TOPIC_NAME = "service.call.police"
BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume():
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([TOPIC_NAME])

    while True:

        messages = c.consume(5, timeout=1.0)

        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")

        await asyncio.sleep(0.01)

def main():
    try:
        asyncio.run(consume_task())
    except KeyboardInterrupt as e:
        print("shutting down")

async def consume_task():
    t2 = asyncio.create_task(consume())
    await t2

if __name__ == "__main__":
    main()
