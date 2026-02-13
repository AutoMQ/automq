from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
import asyncio
import json

async def inspect():
    bootstrap = 'localhost:9092'
    topic = 'iot_telemetry'
    
    print(f"Connecting to {bootstrap}...")
    try:
        # Check Topics
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
        await admin.start()
        topics = await admin.list_topics()
        print(f"Topics: {topics}")
        await admin.close()
        
        if topic not in topics:
            print(f"ERROR: Topic {topic} not found!")
            return

        # Check Offsets
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id='debug_group',
            auto_offset_reset='earliest'
        )
        await consumer.start()
        
        partitions = consumer.partitions_for_topic(topic)
        print(f"Partitions: {partitions}")
        
        total_count = 0
        for p in partitions:
            tp = list(consumer.assignment())[0] # Just dummy, we need low level access
            # AIOKafka high level API doesn't expose end_offsets directly easily without assignment
            # Let's use seek_to_end methodology
            pass 
        
        # Simpler approach: consume one message to see if data exists? 
        # Or better: use Admin Client to describe topic configs?
        
        # Actually, let's just consume all messages with a timeout to count them (approx)
        # Or get highwater mark via underlying client?
        
        # Revert to simple "Get end offsets"
        # Consumer needs to be assigned to partitions to get end_offsets
        from kafka import TopicPartition
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        
        end_offsets = await consumer.end_offsets(tps)
        total = sum(end_offsets.values())
        print(f"Total Offsets (Highwater): {total}")
        
        await consumer.stop()
        
    except Exception as e:
        print(f"Debug failed: {e}")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(inspect())
