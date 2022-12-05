import pytest

from jupyterlabcontroller.models.v1.prepuller import Image
from jupyterlabcontroller.services.prepuller.arbitrator import (
    PrepullerArbitrator,
)


@pytest.mark.asyncio
async def test_get_menu_images(
    prepuller_arbitrator: PrepullerArbitrator,
) -> None:
    r = await prepuller_arbitrator.get_menu_images()
    assert "recommended" in r.menu
    assert type(r.menu["recommended"]) is Image
    assert r.menu["recommended"].digest == "sha256:5678"


@pytest.mark.asyncio
async def test_get_prepulls(prepuller_arbitrator: PrepullerArbitrator) -> None:
    r = await prepuller_arbitrator.get_prepulls()
    assert r.config.docker is not None
    assert r.config.docker.repository == "library/sketchbook"
    assert (
        r.images.prepulled[0].path
        == "lighthouse.ceres/library/sketchbook:recommended@sha256:5678"
    )
    assert r.nodes[0].name == "node1"


@pytest.mark.asyncio
async def test_run_prepuller(
    prepuller_arbitrator: PrepullerArbitrator,
) -> None:
    # We need an executor here
    pass  # FIXME
