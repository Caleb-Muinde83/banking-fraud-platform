import uuid
import time
import asyncio
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.middleware.kafka_producer import send_telemetry_event

# ---------------------------------------------------------------------------
# Strong-reference registry for fire-and-forget background tasks.
#
# asyncio.create_task() only keeps a WEAK reference to the Task internally.
# If the caller doesn't hold its own strong reference somewhere, the task is
# eligible for garbage collection before the event loop ever runs it -- this
# was the root cause of api_requests sitting at zero messages despite live
# traffic. Keeping tasks in this module-level set (and discarding them via
# add_done_callback once they finish) keeps them alive for their full
# lifetime without ever needing to be awaited by the request path.
# ---------------------------------------------------------------------------
_background_tasks: set[asyncio.Task] = set()


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
            #
            # IMPORTANT: we keep a strong reference to the created Task in
            # _background_tasks until it completes. Without this, the task
            # can be garbage-collected before asyncio.to_thread() ever runs
            # send_telemetry_event, silently dropping the event with no
            # exception raised anywhere in this try/except.
            try:
                task = asyncio.create_task(
                    asyncio.to_thread(send_telemetry_event, telemetry_payload)
                )
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)
            except Exception as e:
                # Prevent logging infrastructure failures from crashing the user experience
                print(f"[CRITICAL LOGGING ERROR] Failed to dispatch telemetry task: {e}")