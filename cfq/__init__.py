import asyncio
import inspect
from typing import Callable, Any

from cloudflare import AsyncCloudflare


class Queue:
    def __init__(
        self,
        api_token: str,
        account_id: str,
        *,
        max_workers: int = 5,
        polling_interval_ms: float = 1_000,
        max_batch_size: int = 10,
        allow_retry: bool = False,
        retry_delay_seconds: int = 0
    ):
        assert 0 < max_batch_size <= 100, 'Cloudflare specifies that the max_batch_size should be between 1 and 100: https://developers.cloudflare.com/queues/configuration/batching-retries/'
        assert max_workers >= 1, 'max_workers must be greater than or equal to 1.'
        assert polling_interval_ms >= 0, 'polling_interval_ms must be greater than or equal to 0.'
        assert retry_delay_seconds >= 0, 'retry_delay_seconds must be greater than or equal to 0.'

        self.api_token = api_token
        self.account_id = account_id
        self.max_workers = max_workers
        self.polling_interval_ms = polling_interval_ms
        self.max_batch_size = max_batch_size
        self.retry_delay_seconds = retry_delay_seconds
        self.allow_retry = allow_retry

        self._consumers = {}
        self._poll_workers = []
        self._stop_event = asyncio.Event()
        self._client = AsyncCloudflare(api_token=api_token)

    async def _poller(self, visibility_timeout_ms, fn, queue_id):
        workers = set()

        print('h')
        while not self._stop_event.is_set():
            resp = await self._client.queues.messages.pull(
                queue_id,
                account_id=self.account_id,
                batch_size=self.max_batch_size,
                visibility_timeout_ms=visibility_timeout_ms
            )

            if not resp:
                await asyncio.sleep(self.polling_interval_ms / 1000.0)
                continue

            for message in resp.messages or []:
                while len(workers) >= self.max_workers:
                    done, pending = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
                    workers = pending
                t = asyncio.create_task(self._handler(message, fn, queue_id))
                workers.add(t)
                t.add_done_callback(workers.discard)

    async def _handler(self, message, fn, queue_id):
        try:
            await fn(message)
        except Exception:
            if self.allow_retry:
                # noinspection PyTypeChecker
                await self._client.queues.messages.ack(
                    queue_id,
                    account_id=self.account_id,
                    retries=[
                        {
                            'delay_seconds': self.retry_delay_seconds,
                            'lease_id': message.lease_id
                        }
                    ]
                )
        else:
            # noinspection PyTypeChecker
            await self._client.queues.messages.ack(
                queue_id,
                account_id=self.account_id,
                acks=[{'lease_id': message.lease_id}]
            )

    def consumer(self, queue_id: str, visibility_timeout_ms: int = 60_000):
        def decorator(fn: Callable[[Any], Any]):
            assert inspect.iscoroutinefunction(fn), 'Consumer function must be a coroutine'

            if queue_id in self._consumers:
                raise ValueError(f"Duplicate consumer for queue_id={queue_id}")

            self._consumers[queue_id] = {
                "fn": fn,
                "visibility_timeout_ms": visibility_timeout_ms,
            }

            return fn
        return decorator

    async def start(self):
        self._stop_event.clear()

        for queue_id, consumer_info in self._consumers.items():
            coro = self._poller(consumer_info['visibility_timeout_ms'], consumer_info['fn'], queue_id)
            self._poll_workers.append(asyncio.create_task(coro))

        await asyncio.gather(*self._poll_workers)

    async def stop(self):
        self._stop_event.set()
        self._poll_workers.clear()

        await asyncio.gather(*self._poll_workers, return_exceptions=True)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

