# Redis BRPOPLPUSH Message Queue Pattern

## Overview

HyperZoekt implements a reliable message queue pattern using Redis's `BRPOPLPUSH` command for processing repository indexing events. This pattern ensures atomic message consumption, prevents message loss during failures, and enables concurrent processing across multiple indexer instances.

## Pattern Description

The BRPOPLPUSH pattern provides reliable message queuing by:

1. **Atomic Operations**: Messages are moved from the main queue to a processing queue atomically
2. **Failure Recovery**: Expired messages are automatically recovered and re-queued
3. **Concurrent Processing**: Multiple consumers can process messages simultaneously
4. **TTL-based Cleanup**: Processing keys expire automatically to prevent permanent message loss

## Implementation Details

### Queue Structure

```
zoekt:repo_events_queue     # Main queue for incoming events
zoekt:processing:*          # Processing keys (one per message being processed)
```

### Message Flow

1. **Message Arrival**: Events are pushed to `zoekt:repo_events_queue`
2. **Atomic Consumption**: `BRPOPLPUSH` moves message to unique processing key
3. **TTL Setting**: Processing key gets TTL for failure recovery
4. **Processing**: Message is deserialized and processed
5. **Acknowledgment**: Processing key is deleted on success
6. **Recovery**: Expired processing keys are recovered periodically

### Key Components

#### EventConsumer

The `EventConsumer` handles Redis queue operations:

```rust
// Atomic message consumption with BRPOPLPUSH
let message: Option<String> = conn.brpoplpush(queue_key, &processing_key, 5.0).await?;

// Set TTL for failure recovery
let _: () = conn.expire(&processing_key, processing_ttl_seconds as i64).await?;

// Success acknowledgment
let _: () = conn.del(&processing_key).await?;
```

#### Recovery Mechanism

Expired messages are recovered by scanning processing keys:

```rust
// Find expired processing keys
let processing_keys: Vec<String> = conn.keys(format!("{}*", processing_key_prefix)).await?;

for key in processing_keys {
    let ttl: i64 = conn.ttl(&key).await?;
    if ttl <= 0 {
        // Recover expired message
        let message: Option<String> = conn.get(&key).await?;
        if let Some(msg) = message {
            let _: () = conn.lpush(queue_key, &msg).await?;
            let _: () = conn.del(&key).await?;
        }
    }
}
```

## Configuration

### Environment Variables

- `REDIS_URL`: Redis connection URL (default: `redis://127.0.0.1:6379`)
- `REDIS_USERNAME`: Optional Redis username
- `REDIS_PASSWORD`: Optional Redis password
- `PROCESSING_TTL_SECONDS`: TTL for processing keys (default: 300 seconds)

### TTL Configuration

The processing TTL determines how long a message can be in processing before being considered failed:

```rust
// Default 5-minute TTL
pub async fn start_consuming(&self) -> Result<(), anyhow::Error> {
    self.start_consuming_with_ttl(300).await
}

// Configurable TTL
pub async fn start_consuming_with_ttl(
    &self,
    processing_ttl_seconds: u64,
) -> Result<(), anyhow::Error>
```

## Benefits

### Reliability
- **No Message Loss**: Failed processing triggers automatic recovery
- **Atomic Operations**: BRPOPLPUSH prevents race conditions
- **TTL-based Recovery**: Expired messages are re-queued automatically

### Scalability
- **Concurrent Processing**: Multiple indexers can consume simultaneously
- **Load Distribution**: Redis handles queue distribution automatically
- **Horizontal Scaling**: Add more indexer instances without configuration changes

### Fault Tolerance
- **Graceful Degradation**: System continues with available indexers
- **Automatic Recovery**: Failed messages are re-queued for retry
- **Process Isolation**: Crashes don't affect other processing instances

## Error Handling

### Message Deserialization Failures
Malformed messages are logged and removed from processing:

```rust
match serde_json::from_str::<RepoEvent>(&event_json) {
    Ok(event) => { /* Process event */ }
    Err(e) => {
        error!("Failed to deserialize event: {}", e);
        let _: () = conn.del(&processing_key).await?; // Remove malformed message
    }
}
```

### Connection Failures
Redis connection errors trigger retry with exponential backoff:

```rust
Err(e) => {
    error!("Event consumption failed: {}", e);
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
}
```

### Repository Cloning Failures
Failed repository clones are logged but don't retry (handled by upstream):

```rust
Err(e) => {
    error!("Failed to clone repository {}: {}", event.repo_name, e);
    return Ok(()); // Don't retry, let zoekt-distributed handle it
}
```

## Monitoring and Observability

### Logging
Key events are logged for monitoring:

- Message consumption: `"Processing message: {}"`
- Successful processing: `"Successfully processed and acknowledged message"`
- Recovery events: `"Recovered expired message and re-queued it: {}"`
- Errors: `"Failed to deserialize event: {}"`

### Metrics
Consider monitoring:

- Queue depth (`LLEN zoekt:repo_events_queue`)
- Processing key count (`KEYS zoekt:processing:*`)
- Recovery frequency
- Processing latency
- Error rates

## Best Practices

### TTL Tuning
- Set TTL based on expected processing time plus buffer
- Monitor for false recoveries (TTL too short)
- Monitor for stuck messages (TTL too long)

### Queue Management
- Implement dead letter queues for repeatedly failing messages
- Monitor queue depth to detect processing bottlenecks
- Use Redis persistence for message durability

### Consumer Management
- Use unique consumer IDs to prevent key collisions
- Implement graceful shutdown to complete processing
- Monitor consumer health and restart failed instances

## Integration with Zoekt-Distributed

This Redis queue pattern integrates with zoekt-distributed's event system:

1. **Event Publication**: zoekt-distributed publishes events to `zoekt:repo_events_queue`
2. **Event Consumption**: HyperZoekt consumes events using BRPOPLPUSH pattern
3. **Processing**: Events trigger repository cloning and indexing
4. **Acknowledgment**: Successful processing removes messages from queue

## Future Enhancements

Potential improvements:

- **Priority Queues**: Use multiple queues for different priority levels
- **Delayed Retry**: Implement exponential backoff for failed messages
- **Batch Processing**: Process multiple messages atomically
- **Metrics Integration**: Add Prometheus metrics for queue monitoring
- **Circuit Breaker**: Implement circuit breaker for Redis failures</content>
<parameter name="filePath">/workspaces/hyperzoekt/doc/REDIS_MESSAGE_QUEUE.md
