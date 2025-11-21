# Part 2 - Redis Durability Analysis

## Question: Redis is not durable. Does this cause inconsistency? How can it be mitigated?

### Analysis

**Yes, Redis's lack of durability can cause significant inconsistencies in our incremental processing system.**

### Potential Problems

1. **Lost Progress Tracking**

   - If Redis restarts/crashes, we lose the record of which days have been processed
   - The system may reprocess already-processed data (duplication) or skip unprocessed data (gaps)
   - `processed_days` set and `last_processed_day` key are lost

2. **Inconsistent State**

   - Processed data may exist in the output directory but Redis shows no processed days
   - System cannot distinguish between "never processed" vs "processed but Redis was cleared"
   - May lead to duplicate aggregations or missed data

3. **Race Conditions**
   - If Redis fails during processing, a day might be partially processed but not marked as complete
   - Airflow might restart processing while Spark job is still running

### Mitigation Strategies

#### 1. **Persistent Redis Configuration**

```bash
# Configure Redis with AOF (Append-Only File) persistence
redis-server --appendonly yes --appendfsync everysec
```

#### 2. **Dual Tracking System**

```python
# Track progress in both Redis AND filesystem
def mark_day_processed(self, day):
    # Redis tracking (fast)
    self.redis_client.sadd(self.processed_key, str(day))

    # File system tracking (durable)
    progress_file = f"{workspace}/data/progress/processed_days.txt"
    with open(progress_file, 'a') as f:
        f.write(f"{day}\n")
```

#### 3. **Recovery Mechanism**

```python
def recover_processed_state(self):
    # If Redis is empty, recover from file system
    processed_from_redis = self.get_processed_days()

    if not processed_from_redis:
        # Recover from file system
        progress_file = f"{workspace}/data/progress/processed_days.txt"
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                processed_days = [int(line.strip()) for line in f if line.strip()]

            # Restore to Redis
            for day in processed_days:
                self.redis_client.sadd(self.processed_key, str(day))
```

#### 4. **Idempotent Processing**

- Make processing operations idempotent so reprocessing doesn't cause issues
- Use UPSERT operations instead of INSERT
- Check for existing processed data before processing

#### 5. **Atomic Operations**

```python
def process_day_atomically(self, day):
    # Use Redis transactions
    pipeline = self.redis_client.pipeline()
    pipeline.multi()

    try:
        # Process data
        result = self.process_day_data(day)

        # Only mark as processed if successful
        if result:
            pipeline.sadd(self.processed_key, str(day))
            pipeline.set(self.last_processed_key, str(day))
            pipeline.execute()
        else:
            pipeline.discard()
    except Exception as e:
        pipeline.discard()
        raise e
```

#### 6. **Health Checks and Monitoring**

```python
def validate_system_consistency(self):
    # Check Redis vs file system consistency
    redis_processed = self.get_processed_days()
    file_processed = self.get_file_system_processed_days()

    if redis_processed != file_processed:
        self.logger.warning("Inconsistency detected between Redis and file system")
        # Trigger recovery process
```

### Implementation in Our System

The current implementation could be enhanced with:

1. **AOF Persistence**: Configure Redis with `appendonly yes`
2. **Backup Tracking**: Write processed day info to a durable file
3. **Startup Recovery**: Check for inconsistencies on system startup
4. **Transaction Safety**: Use Redis pipelines for atomic operations

### Conclusion

While Redis's in-memory nature provides excellent performance, the lack of durability requires careful design patterns to maintain consistency. The mitigation strategies above ensure that temporary Redis failures don't compromise data integrity or processing completeness.
