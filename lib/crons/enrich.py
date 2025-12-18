"""Enrichment tasks for dataset ingestion and embedding generation."""
import asyncio
from datetime import datetime, timedelta

from celery import shared_task

from lib.core.container import container


@shared_task(name="enrich.fetch_hf_datasets")
def fetch_hf_datasets(limit: int = 1000, days_back: int = 1):
    """
    Fetch latest datasets from HuggingFace.

    Args:
        limit: Maximum number of datasets to fetch
        days_back: Fetch datasets modified in the last N days
    """
    logger = container.logger
    logger.info(f"Starting HuggingFace dataset fetch: limit={limit}, days_back={days_back}")

    min_date = datetime.utcnow() - timedelta(days=days_back)

    async def _fetch():
        hf_client = container.hf_client
        total_fetched = 0

        async for batch in hf_client.fetch_latest_datasets(
            limit=limit,
            batch_size=1000,
            min_last_modified=min_date
        ):
            logger.info(f"Received batch of {len(batch)} datasets from HuggingFace")
            total_fetched += len(batch)

        return total_fetched

    total = asyncio.run(_fetch())
    logger.info(f"Completed HuggingFace fetch: {total} datasets retrieved")
    return {"total_fetched": total, "source": "huggingface"}


@shared_task(name="enrich.fetch_kaggle_seed")
def fetch_kaggle_seed(batch_size: int = 1000, force_redownload: bool = False):
    """
    Phase 1: Fetch initial seed from Meta Kaggle CSV.

    This downloads all ~50,000+ datasets with minimal metadata.
    Should be run once initially, then occasionally to refresh.

    Args:
        batch_size: Number of datasets per batch
        force_redownload: Force re-download of Meta Kaggle CSV
    """
    logger = container.logger
    logger.info(f"Starting Kaggle seed fetch: batch_size={batch_size}")

    async def _fetch_seed():
        from lib.services.enrichment.kaggle_parser import KaggleClient

        kaggle_client = KaggleClient()
        total_fetched = 0

        async for batch in kaggle_client.fetch_initial_seed(
            batch_size=batch_size,
            force_redownload=force_redownload
        ):
            logger.info(f"Received batch of {len(batch)} datasets from Meta Kaggle CSV")
            total_fetched += len(batch)

            # TODO: Save to database (will be implemented with repository layer)
            # Example: await dataset_repository.bulk_insert_meta(batch)

        return total_fetched

    total = asyncio.run(_fetch_seed())
    logger.info(f"Completed Kaggle seed fetch: {total} datasets retrieved")
    return {"total_fetched": total, "source": "kaggle_meta_csv"}


@shared_task(name="enrich.fetch_kaggle_latest")
def fetch_kaggle_latest(limit: int = 100, sort_by: str = 'updated'):
    """
    Phase 3: Fetch latest datasets from Kaggle API.

    For daily incremental updates - gets newest/updated datasets.

    Args:
        limit: Maximum number of datasets to fetch
        sort_by: Sort order ('updated', 'hottest', 'votes', 'active')
    """
    logger = container.logger
    logger.info(f"Starting Kaggle latest fetch: limit={limit}, sort_by={sort_by}")

    async def _fetch_latest():
        from lib.services.enrichment.kaggle_parser import KaggleClient

        kaggle_client = KaggleClient()
        total_fetched = 0

        async for batch in kaggle_client.fetch_latest_datasets(
            limit=limit,
            sort_by=sort_by
        ):
            logger.info(f"Received batch of {len(batch)} enriched datasets from Kaggle API")
            total_fetched += len(batch)

            # TODO: Save/update in database (will be implemented with repository layer)
            # Example: await dataset_repository.bulk_upsert_enriched(batch)

        return total_fetched

    total = asyncio.run(_fetch_latest())
    logger.info(f"Completed Kaggle latest fetch: {total} datasets retrieved")
    return {"total_fetched": total, "source": "kaggle_api"}


@shared_task(name="enrich.enrich_kaggle_datasets")
def enrich_kaggle_datasets(dataset_refs: list[str], batch_size: int = 10):
    """
    Phase 2: Enrich specific Kaggle datasets with detailed metadata.

    Background worker task to gradually enrich datasets from Phase 1.
    Should be called with dataset references (owner/dataset-name).

    Args:
        dataset_refs: List of dataset references to enrich
        batch_size: Process N datasets at a time
    """
    logger = container.logger
    logger.info(f"Starting enrichment for {len(dataset_refs)} Kaggle datasets")

    async def _enrich_batch():
        from lib.services.enrichment.kaggle_parser import KaggleClient

        kaggle_client = KaggleClient()
        enriched_count = 0
        failed_count = 0

        for i in range(0, len(dataset_refs), batch_size):
            batch = dataset_refs[i:i + batch_size]

            for ref in batch:
                try:
                    enriched_dto = await kaggle_client.enrich_dataset_by_ref(ref)

                    if enriched_dto:
                        enriched_count += 1
                        # TODO: Save enriched data to database
                        # Example: await dataset_repository.update_enriched(enriched_dto)
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to enrich dataset: {ref}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error enriching {ref}: {e}")

            logger.info(
                f"Enriched batch {i // batch_size + 1}: "
                f"{enriched_count} success, {failed_count} failed"
            )

        return enriched_count, failed_count

    enriched, failed = asyncio.run(_enrich_batch())
    logger.info(f"Enrichment completed: {enriched} enriched, {failed} failed")
    return {"enriched": enriched, "failed": failed}


@shared_task(name="enrich.generate_embeddings")
def generate_embeddings(dataset_ids: list[str]):
    """
    Generate embeddings for datasets.

    Args:
        dataset_ids: List of dataset IDs to generate embeddings for
    """
    logger = container.logger
    logger.info(f"Generating embeddings for {len(dataset_ids)} datasets")

    # TODO: Implement embedding generation logic
    # This will be implemented when the repository layer is ready

    logger.info("Embedding generation completed")
    return {"processed": len(dataset_ids)}
