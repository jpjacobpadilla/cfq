import time
import asyncio
import logging
import inspect
import contextlib
from typing import Callable, Any, Awaitable

from cloudflare import AsyncCloudflare
from cloudflare.types.queues.message_pull_response import Message


class CFQ:
    def __init__(
        self,
        api_token: str,
        account_id: str,
        *,
        max_workers: int = 10,
        polling_interval_ms: float = 1_000,
        max_batch_size: int = 10,
        allow_retry: bool = True,
        retry_delay_seconds: int = 0,
        heartbeat_interval_seconds: int = 0,
        **kwargs,
    ):
        self.api_token = api_token
        self.account_id = account_id
        self.max_workers = max_workers
        self.polling_interval_ms = polling_interval_ms
        self.max_batch_size = max_batch_size
        self.allow_retry = allow_retry
        self.retry_delay_seconds = retry_delay_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

        self._consumers = {}
        self._poll_workers = []
        self._client = None
        self._heartbeat_task = None
        self.messages_processed = 0
        self._stop_event = asyncio.Event()

        self.log = kwargs.get('logger') or logging.getLogger('cfq')

    def consumer(self, queue_id: str, visibility_timeout_ms: int = 60_000):
        def decorator(fn: Callable[[Message], Awaitable[Any]]):
            assert inspect.iscoroutinefunction(fn), 'Consumer function must be a coroutine'

            if queue_id in self._consumers:
                raise ValueError(f'Duplicate consumer for queue_id={queue_id}')

            self._consumers[queue_id] = {
                'fn': fn,
                'visibility_timeout_ms': visibility_timeout_ms,
            }

            return fn

        return decorator

    async def _poller(
        self,
        visibility_timeout_ms: int,
        fn: Callable[[Message], Awaitable[Any]],
        queue_id: str,
    ) -> None:
        handler_name = getattr(fn, '__name__', str(fn))
        workers = set()

        while not self._stop_event.is_set():
            resp = await self._client.queues.messages.pull(
                queue_id,
                account_id=self.account_id,
                batch_size=self.max_batch_size,
                visibility_timeout_ms=visibility_timeout_ms,
            )

            if not resp.messages:
                await asyncio.sleep(self.polling_interval_ms / 1000.0)
                continue

            self.log.info(
                f'Pulled {len(resp.messages)} message{"s" if len(resp.messages) > 1 else ""} from queue: {queue_id}'
            )

            for message in resp.messages or []:
                while len(workers) >= self.max_workers:
                    done, pending = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
                    workers = pending
                t = asyncio.create_task(self._handler(message, fn, queue_id, handler_name))
                workers.add(t)
                t.add_done_callback(workers.discard)

    async def _handler(
        self,
        message: Message,
        fn: Callable[[Message], Awaitable[Any]],
        queue_id: str,
        handler_name: str,
    ) -> None:
        try:
            start = time.perf_counter()
            await fn(message)
            runtime = (time.perf_counter() - start) * 1000

            self.log.info(f"Task finished | consumer: '{handler_name}' runtime: {runtime:.2f} ms")

            self.messages_processed += 1

            await self._client.queues.messages.ack(
                queue_id,
                account_id=self.account_id,
                acks=[{'lease_id': message.lease_id}],
            )

        except Exception as e:
            if self.allow_retry:
                self.log.info(f'Task failed, retrying | error: {e}')
                await self._client.queues.messages.ack(
                    queue_id,
                    account_id=self.account_id,
                    retries=[
                        {
                            'delay_seconds': self.retry_delay_seconds,
                            'lease_id': message.lease_id,
                        }
                    ],
                )
            else:
                self.log.info(f'Task failed, not retrying | error: {e}')

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.heartbeat_interval_seconds)
            except asyncio.TimeoutError:
                self.log.info(
                    f'Heartbeat | Processed {self.messages_processed} message{"s" if self.messages_processed > 1 else ""} in last {self.heartbeat_interval_seconds} seconds'
                )
                self.messages_processed = 0


    async def start(self) -> None:
        self._client = AsyncCloudflare(api_token=self.api_token)
        self._stop_event.clear()

        for queue_id, consumer_info in self._consumers.items():
            coro = self._poller(consumer_info['visibility_timeout_ms'], consumer_info['fn'], queue_id)
            self._poll_workers.append(asyncio.create_task(coro))

        if self.heartbeat_interval_seconds and self.heartbeat_interval_seconds > 0:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name='heartbeat')

        self.log.info(f'Starting consumer | max workers: {self.max_workers} consumers: {len(self._consumers)}')

        await asyncio.gather(*self._poll_workers)

    async def stop(self) -> None:
        self._stop_event.set()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

        if self._poll_workers:
            await asyncio.gather(*self._poll_workers, return_exceptions=True)
            self._poll_workers.clear()

        self._client = None
