"""Test fixtures for jupyterlab-controller tests."""

from __future__ import annotations

import asyncio
from copy import copy
from os.path import dirname
from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import AsyncClient
from safir.kubernetes import initialize_kubernetes

from jupyterlabcontroller.config import Config
from jupyterlabcontroller.main import create_app
from jupyterlabcontroller.models.context import Context
from jupyterlabcontroller.services.prepull_executor import PrepullExecutor

from .settings import TestObjectFactory, test_object_factory
from .support.mockdocker import MockDockerStorageClient
from .support.mockk8s import MockK8sStorageClient

_here = dirname(__file__)

CONFIG_DIR = f"{_here}/configs/standard"


@pytest.fixture
def obj_factory() -> TestObjectFactory:
    filename = f"{CONFIG_DIR}/test_objects.json"
    test_object_factory.initialize_from_file(filename)
    return test_object_factory


@pytest.fixture
def config() -> Config:
    """Change the test application configuration to point at a file that
    replaces the YAML that would usually be mounted into the container at
    ``/etc/nublado/config.yaml``.  For testing and standalone purposes, if
    the filename is not the standard location, we expect the Docker
    credentials (if any) to be in ``docker_config.json`` in the same directory
    as ``config.yaml``, and we expect objects used in testing to be in
    ``test_objects.json`` in that directory.
    """
    return Config.from_file(f"{CONFIG_DIR}/config.yaml")


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Increase the scope of the event loop to the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def app() -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    app = create_app(config_dir=CONFIG_DIR)
    async with LifespanManager(app):
        yield app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client


@pytest_asyncio.fixture
def k8s_storage_client(
    obj_factory: TestObjectFactory,
) -> MockK8sStorageClient:
    return MockK8sStorageClient(test_obj=obj_factory)


@pytest_asyncio.fixture
def docker_storage_client(
    obj_factory: TestObjectFactory,
) -> MockDockerStorageClient:
    return MockDockerStorageClient(test_obj=obj_factory)


@pytest_asyncio.fixture
async def context(
    config: Config,
    client: AsyncClient,
    obj_factory: TestObjectFactory,
    k8s_storage_client: MockK8sStorageClient,
    docker_storage_client: MockDockerStorageClient,
) -> Context:
    """Return a ``Context`` configured to supply dependencies."""
    # Force K8s configuration to load
    await initialize_kubernetes()
    cc = Context.initialize(config=config, http_client=client)
    # Patch container with storage mocks
    cc.k8s_client = k8s_storage_client
    cc.docker_client = docker_storage_client
    # Let's pretend we have some running servers already
    cc.user_map = obj_factory.usermap
    return cc


@pytest_asyncio.fixture
async def prepull_executor(
    context: Context,
) -> PrepullExecutor:
    """Return a PrepullExecutor"""
    pe = PrepullExecutor.initialize(context=context)
    return pe


@pytest_asyncio.fixture
async def user_context(
    context: Context, obj_factory: TestObjectFactory
) -> Context:
    """Return Context with user data."""
    cp = copy(context)
    cp.token = "token-of-affection"
    cp.token_scopes = ["exec:notebook"]
    cp.user = obj_factory.userinfos[0]
    cp.namespace = f"{context.namespace}-{cp.user.username}"
    return cp


@pytest_asyncio.fixture
async def admin_context(
    context: Context, obj_factory: TestObjectFactory
) -> Context:
    """Return Context with user data."""
    cp = copy(context)
    cp.token = "token-of-authority"
    cp.token_scopes = ["admin:jupyterlab"]
    cp.user = obj_factory.userinfos[1]
    cp.namespace = f"{context.namespace}-{cp.user.username}"
    return cp
