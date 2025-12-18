from uuid import UUID

from sqlalchemy import select, update, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.dataset import Dataset, EnrichmentStatus
from lib.repositories.base import BaseRepository


class DatasetRepository(BaseRepository[Dataset]):
    """Repository for dataset operations."""

    def __init__(self, session: AsyncSession):
        super().__init__(Dataset, session)

    async def get_by_external_id(
        self, source_name: str, external_id: str
    ) -> Dataset | None:
        """Get dataset by source and external ID."""
        result = await self.session.execute(
            select(Dataset).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.external_id == external_id
                )
            )
        )
        return result.scalar_one_or_none()

    async def upsert(self, dataset: Dataset) -> Dataset:
        """Insert or update dataset by (source_name, external_id)."""
        stmt = insert(Dataset).values(
            source_name=dataset.source_name,
            external_id=dataset.external_id,
            title=dataset.title,
            url=dataset.url,
            description=dataset.description,
            tags=dataset.tags,
            license=dataset.license,
            file_formats=dataset.file_formats,
            total_size_bytes=dataset.total_size_bytes,
            column_names=dataset.column_names,
            row_count=dataset.row_count,
            download_count=dataset.download_count,
            view_count=dataset.view_count,
            like_count=dataset.like_count,
            source_created_at=dataset.source_created_at,
            source_updated_at=dataset.source_updated_at,
            embedding=dataset.embedding,
            static_score=dataset.static_score,
            is_active=dataset.is_active,
            enrichment_status=dataset.enrichment_status,
            enrichment_attempts=dataset.enrichment_attempts,
            last_enrichment_error=dataset.last_enrichment_error,
            last_enriched_at=dataset.last_enriched_at,
            last_checked_at=dataset.last_checked_at,
            source_meta=dataset.source_meta
        ).on_conflict_do_update(
            index_elements=['source_name', 'external_id'],
            set_={
                'title': dataset.title,
                'url': dataset.url,
                'description': dataset.description,
                'tags': dataset.tags,
                'license': dataset.license,
                'file_formats': dataset.file_formats,
                'total_size_bytes': dataset.total_size_bytes,
                'column_names': dataset.column_names,
                'row_count': dataset.row_count,
                'download_count': dataset.download_count,
                'view_count': dataset.view_count,
                'like_count': dataset.like_count,
                'source_created_at': dataset.source_created_at,
                'source_updated_at': dataset.source_updated_at,
                'embedding': dataset.embedding,
                'static_score': dataset.static_score,
                'is_active': dataset.is_active,
                'enrichment_status': dataset.enrichment_status,
                'last_enriched_at': dataset.last_enriched_at,
                'source_meta': dataset.source_meta,
                'updated_at': func.now()
            }
        ).returning(Dataset)

        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one()

    async def bulk_upsert(self, datasets: list[Dataset]) -> int:
        """Bulk insert or update datasets."""
        if not datasets:
            return 0

        values = [
            {
                'source_name': d.source_name,
                'external_id': d.external_id,
                'title': d.title,
                'url': d.url,
                'description': d.description,
                'tags': d.tags,
                'license': d.license,
                'file_formats': d.file_formats,
                'total_size_bytes': d.total_size_bytes,
                'column_names': d.column_names,
                'row_count': d.row_count,
                'download_count': d.download_count,
                'view_count': d.view_count,
                'like_count': d.like_count,
                'source_created_at': d.source_created_at,
                'source_updated_at': d.source_updated_at,
                'embedding': d.embedding,
                'static_score': d.static_score,
                'is_active': d.is_active,
                'enrichment_status': d.enrichment_status,
                'enrichment_attempts': d.enrichment_attempts,
                'last_enrichment_error': d.last_enrichment_error,
                'last_enriched_at': d.last_enriched_at,
                'last_checked_at': d.last_checked_at,
                'source_meta': d.source_meta
            }
            for d in datasets
        ]

        stmt = insert(Dataset).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['source_name', 'external_id'],
            set_={
                'title': stmt.excluded.title,
                'url': stmt.excluded.url,
                'description': stmt.excluded.description,
                'tags': stmt.excluded.tags,
                'license': stmt.excluded.license,
                'file_formats': stmt.excluded.file_formats,
                'total_size_bytes': stmt.excluded.total_size_bytes,
                'column_names': stmt.excluded.column_names,
                'row_count': stmt.excluded.row_count,
                'download_count': stmt.excluded.download_count,
                'view_count': stmt.excluded.view_count,
                'like_count': stmt.excluded.like_count,
                'source_created_at': stmt.excluded.source_created_at,
                'source_updated_at': stmt.excluded.source_updated_at,
                'is_active': stmt.excluded.is_active,
                'source_meta': stmt.excluded.source_meta,
                'updated_at': func.now()
            }
        )

        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount

    async def get_pending_for_enrichment(
        self,
        source_name: str,
        limit: int = 100,
        max_attempts: int = 3
    ) -> list[Dataset]:
        """Get datasets pending API enrichment for specific source."""
        result = await self.session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.source_name == source_name,
                    or_(
                        Dataset.enrichment_status == (
                            EnrichmentStatus.MINIMAL.value
                        ),
                        Dataset.enrichment_status == (
                            EnrichmentStatus.PENDING.value
                        )
                    ),
                    Dataset.enrichment_attempts < max_attempts,
                    Dataset.is_active
                )
            )
            .order_by(Dataset.created_at.asc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_for_embedding_generation(
        self, limit: int = 100
    ) -> list[Dataset]:
        """Get datasets ready for embedding generation."""
        result = await self.session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.enrichment_status == (
                        EnrichmentStatus.ENRICHED.value
                    ),
                    Dataset.embedding.is_(None),
                    Dataset.is_active
                )
            )
            .limit(limit)
        )
        return list(result.scalars().all())

    async def mark_enriching(self, dataset_id: UUID) -> None:
        """Mark dataset as currently enriching."""
        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.ENRICHING.value,
                enrichment_attempts=Dataset.enrichment_attempts + 1
            )
        )
        await self.session.flush()

    async def mark_enriched(
        self, dataset_id: UUID, embedding: list[float] | None = None
    ) -> None:
        """Mark dataset as fully enriched."""
        values = {
            'enrichment_status': EnrichmentStatus.ENRICHED.value,
            'last_enriched_at': func.now()
        }
        if embedding is not None:
            values['embedding'] = embedding

        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(**values)
        )
        await self.session.flush()

    async def mark_failed(
        self, dataset_id: UUID, error_message: str
    ) -> None:
        """Mark dataset as failed enrichment."""
        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.FAILED.value,
                last_enrichment_error=error_message,
                is_active=False
            )
        )
        await self.session.flush()

    async def count_by_source(self, source_name: str) -> int:
        """Count datasets by source."""
        result = await self.session.execute(
            select(func.count(Dataset.id)).where(
                Dataset.source_name == source_name
            )
        )
        return result.scalar_one()

    async def count_by_status(
        self, source_name: str, status: EnrichmentStatus
    ) -> int:
        """Count datasets by source and enrichment status."""
        result = await self.session.execute(
            select(func.count(Dataset.id)).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.enrichment_status == status.value
                )
            )
        )
        return result.scalar_one()

    async def get_stats_by_source(self, source_name: str) -> dict:
        """Get statistics for a specific source."""
        total = await self.count_by_source(source_name)
        minimal = await self.count_by_status(
            source_name, EnrichmentStatus.MINIMAL
        )
        pending = await self.count_by_status(
            source_name, EnrichmentStatus.PENDING
        )
        enriching = await self.count_by_status(
            source_name, EnrichmentStatus.ENRICHING
        )
        enriched = await self.count_by_status(
            source_name, EnrichmentStatus.ENRICHED
        )
        failed = await self.count_by_status(
            source_name, EnrichmentStatus.FAILED
        )

        return {
            'source': source_name,
            'total': total,
            'minimal': minimal,
            'pending': pending,
            'enriching': enriching,
            'enriched': enriched,
            'failed': failed
        }
