from typing import List

from fastapi import Depends

from ..dependencies.config import configuration_dependency
from ..dependencies.docker import docker_dependency
from ..models.v1.domain.config import Config as GlobalConfig
from ..models.v1.domain.docker import DockerMap
from ..models.v1.external.prepuller import Config


class DockerClient:
    async def authenticate() -> None:
        pass

    async def list_tags(
        cfg: GlobalConfig = Depends(configuration_dependency),
        credentials: DockerMap = Depends(docker_dependency),
    ) -> List[str]:
        config: Config = cfg.prepuller.config
        path = config.path
        endpoint = f"{path}/tags/list"
        _ = endpoint
