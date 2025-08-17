# CFQ

A high-level Python client for consuming messages from Cloudflare Queues with async workers.

```python
from cfq import CFQ

client = CFQ(
    api_token="your_cloudflare_api_token",
    account_id="your_account_id",
)

@client.consumer(queue_id="your_queue_id")
async def process_messages(message):
    # Messages will be automatically ACKed on success 
    # or sent back to the queue to be retried on exceptions.

await client.start()
```

CFQ's API design is inspired by [TaskIQ](https://github.com/taskiq-python/taskiq) and [Celery](https://github.com/celery/celery), adapted for Cloudflare Queues.

### Installation

```bash
uv add cfq
```

### CFQ Parameters

| Parameter | Type | Default    | Description                                                   |
|-----------|------|------------|---------------------------------------------------------------|
| `api_token` | `str` | *required* | Cloudflare API token with Queues permissions                  |
| `account_id` | `str` | *required* | Your Cloudflare account identifier                            |
| `max_workers` | `int` | `10`       | Maximum concurrent message handlers (async workers)           |
| `polling_interval_ms` | `float` | `1000`     | Polling interval in milliseconds when queue is empty          |
| `flush_interval_ms` | `float` | `1000`     | Interval in milliseconds to send acks / retries to Cloudflare |
| `max_batch_size` | `int` | `10`       | Messages to pull per request                                  |
| `allow_retry` | `bool` | `True`     | Whether to retry failed messages                              |
| `retry_delay_seconds` | `int` | `0`        | Delay before retrying failed messages                         |
| `heartbeat_interval_seconds` | `int` | `0`        | Heartbeat logging interval (0 = disabled)                     |
| `logger` | `Logger` | `None`     | Custom logger (defaults to "cfq" logger)                      |
| `httpx_logs` | `bool` | `False`    | Enable httpx debug logs (disabled by default)|

### Consumer Decorator Parameters

```python
from cloudflare.types.queues.message_pull_response import Message

@client.consumer(queue_id="queue_id", visibility_timeout_ms=60000)
async def my_consumer(message: Message):
    # Your message processing logic
    pass
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `queue_id` | `str` | *required* | The Cloudflare Queue ID to consume from |
| `visibility_timeout_ms` | `int` | `60000` | Message visibility timeout in milliseconds |


### Multiple Queue Consumers

```python
client = CFQ(
    api_token="your_token",
    account_id="your_account_id",
    max_workers=20,  # Shared across all consumers
)

@client.consumer(queue_id="email_queue_id")
async def handle_emails(message: Message):
    # Process email messages
    await send_email(message.body)

@client.consumer(queue_id="webhook_queue_id", visibility_timeout_ms=30000)
async def handle_webhooks(message: Message):
    # Process webhook messages with shorter timeout
    await process_webhook(message.body)

await client.start()
```

### Custom Configuration Example

```python
client = CFQ(
    api_token="your_token",
    account_id="your_account_id",
    max_workers=5,
    polling_interval_ms=500,  # Poll every 500ms
    max_batch_size=20,        # Pull up to 20 messages at once
    allow_retry=True,
    retry_delay_seconds=30,   # Wait 30s before retry
    heartbeat_interval_seconds=60,  # Log heartbeat every minute
)
```

### Error Handling

CFQ automatically handles message acknowledgment and retries:

- **Success**: Messages are automatically ACKed after successful processing
- **Failure with retry enabled**: Failed messages are retried with configurable delay
- **Failure with retry disabled**: Failed messages are discarded
- **Worker limits**: New messages wait for available workers when `max_workers` is reached

### Monitoring

Enable heartbeat logging to monitor processing rates:

```python
client = CFQ(
    # ... other config
    heartbeat_interval_seconds=30,  # Log every 30 seconds
)
```

This will output logs like:
```
INFO:cfq:Heartbeat | Processed 42 messages in last 30 seconds
```