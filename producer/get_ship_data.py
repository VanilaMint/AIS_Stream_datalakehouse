import asyncio
import websockets
import json
import os
from aiokafka import AIOKafkaProducer


API_KEY = os.getenv("AIS_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:19092")
KAFKA_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "ship_logs_raw")

if not API_KEY:
    raise ValueError("FATAL ERROR: AIS_API_KEY environment variable is missing!")

async def connect_ais_stream():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    await producer.start()
    print(f"Connected to Kafka broker at {KAFKA_BROKER}")

    url = "wss://stream.aisstream.io/v0/stream"
    print(f"Attempting to connect to {url}...")
    
    try:
        async with websockets.connect(url) as websocket:
            print("Successfully connected to AIS! Sending subscription...")
            
            subscribe_message = {
                "APIKey": API_KEY,  
                "BoundingBoxes": [[[-90, -180], [90, 180]]], 
                "FilterMessageTypes": ["PositionReport","BaseStationReport","ShipStaticData"] 
            }

            await websocket.send(json.dumps(subscribe_message))
            print("Subscription sent! Forwarding data to Kafka...")

            async for message_json in websocket:
                message = json.loads(message_json)
                
                if "error" in message:
                    print(f"SERVER ERROR: {message['error']}")
                    continue 

                await producer.send_and_wait(KAFKA_TOPIC, message)

                ship_name = message.get("MetaData", {}).get("ShipName", "Unknown Ship")
                print(f"Sent to Kafka: {ship_name}")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\nCONNECTION CLOSED: The server dropped the connection. Reason: {e}")
    except Exception as e:
        print(f"\nNETWORK ERROR: Failed to connect. Error: {e}")
    finally:
        await producer.stop()
        print("Kafka producer stopped.")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())