import uuid
import time
import asyncio
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.middleware.kafka_producer import send_telemetry_event

class KafkaRequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 1. Capture request incoming metadata
        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        ip_address = request.client.host if request.client else "127.0.0.1"
        user_agent = request.headers.get("user-agent", "Unknown-Device")
        method = request.method
        endpoint = request.url.path

        status_code = 500  # Default fallback in case of an application crash

        # 2. Process the actual API request
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as e:
            # Retain the excellent 500 error logging logic from your original code
            status_code = 500
            raise e
        finally:
            # 3. Construct our strict Avro telemetry contract payload
            telemetry_payload = {
                "request_id": request_id,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "endpoint": endpoint,
                "method": method,
                "status_code": int(status_code),
                "timestamp": int(time.time() * 1000)
            }

            # 4. Offload the synchronous Avro validation and Kafka producer call 
            # to a background thread so the API response isn't blocked.
            try:
                asyncio.create_task(
                    asyncio.to_thread(send_telemetry_event, telemetry_payload)
                )
            except Exception as e:
                # Prevent logging infrastructure failures from crashing the user experience
                print(f"[CRITICAL LOGGING ERROR] Failed to dispatch telemetry task: {e}")