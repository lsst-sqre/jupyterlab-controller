from typing import List

from httpx import AsyncClient
from jinja2 import Template
from structlog.stdlib import BoundLogger

from ..config import LabSizeDefinitions
from ..constants import SPAWNER_FORM_TEMPLATE
from ..models.domain.form import FormSize
from .prepuller import PrepullerManager

DROPDOWN_SENTINEL_VALUE = "use_image_from_dropdown"


class FormManager:
    def __init__(
        self,
        username: str,
        prepuller_manager: PrepullerManager,
        logger: BoundLogger,
        http_client: AsyncClient,
        lab_sizes: LabSizeDefinitions,
    ):
        self.username = username
        self.prepuller_manager = prepuller_manager
        self.logger = logger
        self.http_client = http_client
        self.lab_sizes = lab_sizes

    def _extract_sizes(self) -> List[FormSize]:
        sz = self.lab_sizes
        return [
            FormSize(
                name=x.title(),
                cpu=str((sz[x]).cpu),
                memory=str((sz[x]).memory),
            )
            for x in sz
        ]

    async def generate_user_lab_form(self) -> str:
        if self.username is None:
            raise RuntimeError("Cannot create user form without user")
        self.logger.info(f"Creating options form for '{self.username}'")
        options_template = Template(SPAWNER_FORM_TEMPLATE)

        pm = self.prepuller_manager
        displayimages = await pm.get_menu_images()
        cached_images = list(displayimages.menu.values())
        all_images = list(displayimages.all.values())
        sizes = self._extract_sizes()
        self.logger.debug(f"cached images: {cached_images}")
        self.logger.debug(f"all images: {all_images}")
        self.logger.debug(f"sizes: {sizes}")
        return options_template.render(
            dropdown_sentinel=DROPDOWN_SENTINEL_VALUE,
            cached_images=cached_images,
            all_images=all_images,
            sizes=sizes,
        )
