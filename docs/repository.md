# Repository Layer

## Overview

Repository layer provides data access abstraction for datasets and enrichment logs.

## DatasetRepository

### Basic Operations

```python
from lib.repositories import DatasetRepository
from lib.models import Dataset, EnrichmentStatus

async with get_session() as session:
    repo = DatasetRepository(session)

    # Get by external ID
    dataset = await repo.get_by_external_id("kaggle", "owner/dataset-name")

    # Upsert (insert or update)
    new_dataset = Dataset(
        source_name="kaggle",
        external_id="owner/dataset-name",
        title="My Dataset",
        url="https://kaggle.com/...",
        enrichment_status=EnrichmentStatus.MINIMAL.value
    )
    result = await repo.upsert(new_dataset)
    await repo.commit()
```

### Bulk Operations

```python
# Bulk upsert
datasets = [Dataset(...), Dataset(...), Dataset(...)]
count = await repo.bulk_upsert(datasets)
await repo.commit()
```

### Enrichment Workflow

```python
# Get datasets pending enrichment
pending = await repo.get_pending_for_enrichment(
    source_name="kaggle",
    limit=100,
    max_attempts=3
)

# Mark as enriching
await repo.mark_enriching(dataset.id)

# ... do enrichment ...

# Mark as enriched
await repo.mark_enriched(dataset.id, embedding=[0.1, 0.2, ...])

# Or mark as failed
await repo.mark_failed(dataset.id, "Error message")

await repo.commit()
```

### Statistics

```python
# Get stats by source
stats = await repo.get_stats_by_source("kaggle")
# Returns: {'source': 'kaggle', 'total': 1000, 'minimal': 500, ...}

# Count by status
count = await repo.count_by_status("kaggle", EnrichmentStatus.ENRICHED)
```

## EnrichmentLogRepository

### Logging Enrichment Attempts

```python
from lib.repositories import EnrichmentLogRepository
from lib.models import EnrichmentStage, EnrichmentResult

async with get_session() as session:
    log_repo = EnrichmentLogRepository(session)

    # Log successful enrichment
    await log_repo.log_enrichment(
        dataset_id=dataset.id,
        stage=EnrichmentStage.API_METADATA,
        result=EnrichmentResult.SUCCESS,
        attempt_number=1,
        duration_ms=1234,
        worker_id="celery@worker1",
        task_id="abc-123"
    )

    # Log failed enrichment
    await log_repo.log_enrichment(
        dataset_id=dataset.id,
        stage=EnrichmentStage.API_METADATA,
        result=EnrichmentResult.FAILED,
        attempt_number=2,
        error_message="Dataset not found",
        error_type="NotFoundError",
        duration_ms=234
    )

    await log_repo.commit()
```

### Analytics

```python
# Get statistics (last 24 hours)
stats = await log_repo.get_stats_by_stage_and_result(hours=24)
# Returns: [{'stage': 'api_metadata', 'result': 'success', 'count': 100, 'avg_duration_ms': 1234.5}, ...]

# Get top errors (last 7 days)
errors = await log_repo.get_top_errors(hours=168, limit=10)
# Returns: [{'error_type': 'NotFoundError', 'count': 42}, ...]

# Get failed logs
failed = await log_repo.get_failed_logs(since=datetime.utcnow() - timedelta(days=1))
```

## Transaction Management

```python
async with get_session() as session:
    repo = DatasetRepository(session)

    try:
        dataset = await repo.upsert(new_dataset)
        await repo.commit()
    except Exception as e:
        await repo.rollback()
        raise
```
