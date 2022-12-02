from typing import Optional

from fastapi import Depends, Header, Request
from httpx import AsyncClient
from kubernetes_asyncio.client import ApiClient
from safir.dependencies.http_client import http_client_dependency
from safir.dependencies.logger import logger_dependency
from structlog.stdlib import BoundLogger

from ..config import Configuration
from ..constants import KUBERNETES_REQUEST_TIMEOUT
from ..models.domain.docker import DockerCredentialsMap
from ..storage.docker import DockerStorageClient
from ..storage.gafaelfawr import GafaelfawrStorageClient
from ..storage.k8s import K8sStorageClient
from ..util import extract_bearer_token
from .config import configuration_dependency
from .credentials import docker_credentials_dependency


class K8sAPIClientDependency:
    def __init__(self) -> None:
        self._api_client: Optional[ApiClient] = None

    def set_state(self, api_client: ApiClient) -> None:
        self._api_client = api_client

    async def __call__(
        self,
    ) -> ApiClient:
        if self._api_client is None:
            self._api_client = ApiClient()
        return self._api_client


k8s_api_client_dependency = K8sAPIClientDependency()


async def k8s_storage_dependency(
    request: Request,
    logger: BoundLogger = Depends(logger_dependency),
    k8s_client: ApiClient = Depends(k8s_api_client_dependency),
) -> K8sStorageClient:
    return K8sStorageClient(
        logger=logger,
        k8s_api=k8s_client,
        timeout=KUBERNETES_REQUEST_TIMEOUT,
    )


async def docker_storage_dependency(
    request=Request,
    config: Configuration = Depends(configuration_dependency),
    http_client: AsyncClient = Depends(http_client_dependency),
    logger: BoundLogger = Depends(logger_dependency),
    credentials: DockerCredentialsMap = Depends(docker_credentials_dependency),
) -> DockerStorageClient:
    return DockerStorageClient(
        host=config.images.registry,
        repository=config.images.repository,
        http_client=http_client,
        logger=logger,
        credentials=credentials.get(config.images.repository),
    )


async def gafaelfawr_storage_dependency(
    authorization: str = Header(...),
    http_client: AsyncClient = Depends(http_client_dependency),
    logger: BoundLogger = Depends(logger_dependency),
) -> GafaelfawrStorageClient:
    token = extract_bearer_token(authorization)
    return GafaelfawrStorageClient(
        token=token, http_client=http_client, logger=logger
    )
