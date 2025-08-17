import time
import asyncio
import logging
import inspect
import contextlib
from collections import defaultdict
from typing import Callable, Any, Awaitable

from cloudflare import AsyncCloudflare, RateLimitError, APIConnectionError
from cloudflare.types.queues.message_pull_response import Message

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def before_sleep_log(logger, level):
    def log_retry(retry_state):
        exc = retry_state.outcome.exception()
        attempt = retry_state.attempt_number
        logger.log(level, f'Retrying Cloudflare request | attempt {attempt} error: {exc}')
    return log_retry


class CFQ:
    def __init__(
        self,
        api_token: str,
        account_id: str,
        *,
        max_workers: int = 10,
        polling_interval_ms: float = 1_000,
        flush_interval_ms: float = 1_000,
        max_batch_size: int = 30,
        allow_retry: bool = True,
        retry_delay_seconds: int = 0,
        heartbeat_interval_seconds: int = 0,
        **kwargs,
    ):
        self.api_token = api_token
        self.account_id = account_id
        self.max_workers = max_workers
        self.polling_interval_ms = polling_interval_ms
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_size = max_batch_size
        self.allow_retry = allow_retry
        self.retry_delay_seconds = retry_delay_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

        self.log = kwargs.get('logger') or logging.getLogger('cfq')
        if not kwargs.get('httpx_logs'):
            logging.getLogger("httpx").setLevel(logging.WARNING)

        self._consumers = {}
        self._poll_workers = []
        self._client = None
        self._heartbeat_task = None
        self._ack_flush = None
        self.messages_processed = 0

        self._stop_event = asyncio.Event()

        self._pending_acks = defaultdict(list)
        self._pending_retries = defaultdict(list)

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

    @retry(
        stop=stop_after_attempt(8),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception_type((RateLimitError, APIConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger('cfq'), logging.WARNING),
    )
    async def _pull_with_retry(self, queue_id: str, **kwargs):
        return await self._client.queues.messages.pull(queue_id, **kwargs)

    @retry(
        stop=stop_after_attempt(8),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception_type((RateLimitError, APIConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger('cfq'), logging.WARNING),
    )
    async def _ack_with_retry(self, queue_id: str, **kwargs):
        return await self._client.queues.messages.ack(queue_id, **kwargs)

    async def _poller(
        self,
        visibility_timeout_ms: int,
        fn: Callable[[Message], Awaitable[Any]],
        queue_id: str,
    ) -> None:
        handler_name = getattr(fn, '__name__', str(fn))
        workers = set()

        while not self._stop_event.is_set():
            try:
                resp = await self._pull_with_retry(
                    queue_id,
                    account_id=self.account_id,
                    batch_size=self.max_batch_size,
                    visibility_timeout_ms=visibility_timeout_ms,
                )
            except Exception as e:
                self.log.error(f'Failed to pull from queue {queue_id}: {e}')
                await asyncio.sleep((self.polling_interval_ms / 1000.0) * 2)
                continue

            if not resp.messages:
                await asyncio.sleep(self.polling_interval_ms / 1000.0)
                continue

            self.log.info(
                f'Pulled {len(resp.messages)} message{"s" if len(resp.messages) > 1 else ""} from queue {queue_id}'
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
            self._pending_acks[queue_id].append({'lease_id': message.lease_id})

        except Exception as e:
            if self.allow_retry:
                self.log.info(f'Task failed, retrying | error: {e}')
                self._pending_retries[queue_id].append(
                    {
                        'delay_seconds': self.retry_delay_seconds,
                        'lease_id': message.lease_id,
                    }
                )
            else:
                self.log.info(f'Task failed, not retrying | error: {e}')

    async def _ack_flusher(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(self.flush_interval_ms / 1000.0)
            for queue_id in self._consumers:
                await self._flush_acks_retries(queue_id)

        for queue_id in self._consumers:
            await self._flush_acks_retries(queue_id)

    async def _flush_acks_retries(self, queue_id: str) -> None:
        if not self._pending_acks[queue_id] and not self._pending_retries[queue_id]:
            return

        try:
            await self._ack_with_retry(
                queue_id,
                account_id=self.account_id,
                acks=self._pending_acks[queue_id],
                retries=self._pending_retries[queue_id],
            )

            ack_count = len(self._pending_acks[queue_id])
            retry_count = len(self._pending_retries[queue_id])

            self._pending_acks[queue_id].clear()
            self._pending_retries[queue_id].clear()

            if ack_count or retry_count:
                self.log.debug(f'Flushed {ack_count} acks, {retry_count} retries for queue {queue_id}')

        except Exception as e:
            self.log.error(f'Failed to flush acks/retries for queue {queue_id} error: {e}')

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

        self._ack_flush = asyncio.create_task(self._ack_flusher())

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

        await self._ack_flush

        self._client = None
        self._ack_flush = None
