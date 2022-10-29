"""Configuration dependency."""
from typing import Optional

from fastapi import Depends
from safir.dependencies.logger import logger_dependency
from structlog.stdlib import BoundLogger

from ..models.v1.consts import CONFIGURATION_PATH
from ..models.v1.domain.config import Config


class ConfigurationDependency:
    _configuration_path: str = CONFIGURATION_PATH
    _config: Optional[Config] = None
    _logger: Optional[BoundLogger] = None

    async def __call__(
        self, logger: BoundLogger = Depends(logger_dependency)
    ) -> Config:
        self._logger = logger
        return self.config()

    def config(self) -> Config:
        if self._config is None:
            assert self._logger is not None  # mypy is dumb
            self._config = Config.from_file(
                self._configuration_path, logger=self._logger
            )
        return self._config

    def set_configuration_path(self, path: str) -> None:
        """Change the settings path and reload."""
        self._configuration_path = path
        assert self._logger is not None  # mypy is dumb
        self._config = Config.from_file(
            self._configuration_path, logger=self._logger
        )


configuration_dependency = ConfigurationDependency()
