import asyncio
from typing import AsyncGenerator
from tenacity import retry, stop_after_attempt, wait_exponential

from lib.core.container import container
from lib.schemas.dataset import KaggleEnrichedDatasetDTO
from ..models import APIConsts
from ..utils import initialize_kaggle_api


class KaggleAPIClient:
    """Handles Kaggle API operations for detailed metadata."""

    def __init__(
        self, throttle_delay: float = APIConsts.DEFAULT_THROTTLE_DELAY
    ):
        self._logger = container.logger
        self.throttle_delay = throttle_delay
        self.api = initialize_kaggle_api()

    async def fetch_latest_datasets(
        self,
        limit: int,
        sort_by: str = 'updated'
    ) -> AsyncGenerator[list[KaggleEnrichedDatasetDTO], None]:
        """Fetches latest datasets with full metadata."""
        fetched_count = 0
        page = 1

        while fetched_count < limit:
            datasets_page = await self._fetch_dataset_list_page(
                page=page, sort_by=sort_by
            )

            if not datasets_page:
                break

            batch = await self._enrich_datasets_batch(
                datasets_page=datasets_page,
                max_count=limit - fetched_count
            )

            if batch:
                fetched_count += len(batch)
                yield batch

            page += 1

            if len(datasets_page) < APIConsts.DEFAULT_PAGE_SIZE:
                break

    async def fetch_single_dataset(
        self, dataset_ref: str
    ) -> KaggleEnrichedDatasetDTO | None:
        """Fetches single dataset with full metadata."""
        loop = asyncio.get_event_loop()

        try:
            dataset_data = await self._fetch_with_retry(
                loop=loop, dataset_ref=dataset_ref
            )
            return KaggleEnrichedDatasetDTO(**dataset_data) if dataset_data else None

        except Exception as e:
            self._logger.error(f"Failed to fetch {dataset_ref}: {e}")
            return None

    async def _fetch_dataset_list_page(self, page: int, sort_by: str) -> list[dict]:
        """Fetches single page of dataset list."""
        self._logger.info(f"Fetching page {page}")

        loop = asyncio.get_event_loop()

        try:
            datasets = await loop.run_in_executor(
                None,
                lambda: self.api.dataset_list(sort_by=sort_by, page=page)
            )
            return datasets or []

        except Exception as e:
            self._logger.error(f"Error fetching page {page}: {e}")
            return []

    async def _enrich_datasets_batch(
        self,
        datasets_page: list[dict],
        max_count: int
    ) -> list[KaggleEnrichedDatasetDTO]:
        """Enriches a batch of datasets with full metadata."""
        batch = []

        for dataset in datasets_page:
            if len(batch) >= max_count:
                break

            dto = await self.fetch_single_dataset(dataset_ref=dataset.ref)

            if dto:
                batch.append(dto)

            await asyncio.sleep(self.throttle_delay)

        return batch

    @retry(
        stop=stop_after_attempt(APIConsts.MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=1,
            min=APIConsts.RETRY_MIN_WAIT,
            max=APIConsts.RETRY_MAX_WAIT
        ),
        reraise=True
    )
    async def _fetch_with_retry(
        self, loop: asyncio.AbstractEventLoop, dataset_ref: str
    ) -> dict | None:
        """Fetches dataset with retry logic."""
        try:
            dataset_obj = await loop.run_in_executor(
                None,
                lambda: self.api.dataset_view(dataset_ref)
            )

            dataset_dict = self._build_dataset_dict(dataset_obj=dataset_obj)
            files_list = await self._fetch_dataset_files(
                loop=loop, dataset_ref=dataset_ref
            )
            dataset_dict['data'] = files_list

            return dataset_dict

        except Exception as e:
            self._logger.warning(f"Error fetching {dataset_ref}: {e}")
            return None

    def _build_dataset_dict(self, dataset_obj: object) -> dict:
        """Builds dataset dict from Kaggle API object."""
        return {
            'ref': dataset_obj.ref,
            'title': dataset_obj.title,
            'subtitle': getattr(dataset_obj, 'subtitle', None),
            'creatorName': dataset_obj.creatorName,
            'totalBytes': dataset_obj.totalBytes,
            'url': dataset_obj.url,
            'createdDate': dataset_obj.createdDate,
            'lastUpdated': dataset_obj.lastUpdated,
            'downloadCount': dataset_obj.downloadCount,
            'voteCount': dataset_obj.voteCount,
            'viewCount': dataset_obj.viewCount,
            'licenseName': getattr(dataset_obj, 'licenseName', None),
            'description': getattr(dataset_obj, 'description', None),
        }

    async def _fetch_dataset_files(
        self, loop: asyncio.AbstractEventLoop, dataset_ref: str
    ) -> list[dict]:
        """Fetches dataset files metadata."""
        try:
            files = await loop.run_in_executor(
                None,
                lambda: self.api.dataset_list_files(dataset_ref).files
            )

            return [self._build_file_dict(file=file) for file in files]

        except Exception as e:
            self._logger.debug(f"Could not fetch files for {dataset_ref}: {e}")
            return []

    def _build_file_dict(self, file: object) -> dict:
        """Builds file dict from Kaggle API file object."""
        file_dict = {
            'name': file.name,
            'size': file.totalBytes,
            'creationDate': getattr(file, 'creationDate', None),
        }

        if hasattr(file, 'columns') and file.columns:
            file_dict['columns'] = [col.name for col in file.columns]

        return file_dict
