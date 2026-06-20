import json
import uuid
import asyncio
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from aiokafka import AIOKafkaProducer

# Global producer variable that we will initialize on API startup
kafka_producer: AIOKafkaProducer = None

class KafkaRequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        
        # 1. Capture Telemetry
        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        method = request.method
        endpoint = request.url.path

        # 2. Process the actual API request
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            # If the application crashes, we still want to log the 500 error!
            status_code = 500
            raise e
        finally:
            # 3. Construct the Event Payload
            log_event = {
                "request_id": request_id,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "method": method,
                "endpoint": endpoint,
                "status_code": status_code
            }
            
            # 4. Produce to Kafka (fire-and-forget)
            if kafka_producer:
                # We use asyncio.create_task so the API response doesn't have to 
                # wait for the network call to Kafka to finish.
                asyncio.create_task(
                    kafka_producer.send_and_wait("api_requests", log_event)
                )

        return response