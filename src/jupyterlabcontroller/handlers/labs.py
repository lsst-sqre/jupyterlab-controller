"""User-facing routes, as defined in sqr-066 (https://sqr-066.lsst.io),
these specifically for lab manipulation"""
from collections.abc import AsyncGenerator
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from safir.dependencies.logger import logger_dependency
from safir.models import ErrorModel
from sse_starlette.sse import ServerSentEvent
from structlog.stdlib import BoundLogger

from ..dependencies.context import context_dependency
from ..dependencies.token import admin_token_dependency, user_token_dependency
from ..models.context import Context
from ..models.v1.lab import LabSpecification, UserData

# FastAPI routers
router = APIRouter()


#
# User routes
#


# Lab Controller API: https://sqr-066.lsst.io/#lab-controller-rest-api
# Prefix: /nublado/spawner/v1/labs


@router.get(
    "/",
    responses={
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    summary="List all users with running labs",
)
async def get_lab_users(
    context: Context = Depends(context_dependency),
    admin_token: str = Depends(admin_token_dependency),
) -> List[str]:
    """Returns a list of all users with running labs."""
    return await context.user_map.running()


@router.get(
    "/{username}",
    response_model=UserData,
    responses={
        404: {"description": "Lab not found", "model": ErrorModel},
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    summary="Status of user",
)
async def get_userdata(
    username: str,
    context: Context = Depends(context_dependency),
    admin_token: str = Depends(admin_token_dependency),
) -> UserData:
    """Returns status of the lab pod for the given user."""
    userdata = context.user_map.get(username)
    if userdata is None:
        raise HTTPException(status_code=404, detail="Not found")
    return userdata


@router.post(
    "/{username}/create",
    responses={
        409: {"description": "Lab exists", "model": ErrorModel},
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    response_class=RedirectResponse,
    status_code=303,
    summary="Create user lab",
)
async def post_new_lab(
    username: str,
    lab: LabSpecification,
    context: Context = Depends(context_dependency),
    user_token: str = Depends(user_token_dependency),
    logger: BoundLogger = Depends(logger_dependency),
) -> str:
    """Create a new Lab pod for a given user"""
    user = await context.get_user()
    token_username = user.username
    if token_username != username:
        raise HTTPException(status_code=403, detail="Forbidden")
    lab_manager = context.lab_manager
    context.logger.debug(f"Received creation request for {username}")
    await lab_manager.create_lab(token=user_token, lab=lab)
    return f"/nublado/spawner/v1/labs/{username}"


@router.delete(
    "/{username}",
    summary="Delete user lab",
    responses={
        404: {"description": "Lab not found", "model": ErrorModel},
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    status_code=202,
)
async def delete_user_lab(
    username: str,
    logger: BoundLogger = Depends(logger_dependency),
    context: Context = Depends(context_dependency),
    admin_token: str = Depends(admin_token_dependency),
) -> None:
    """Stop a running pod."""
    lab_manager = context.lab_manager
    await lab_manager.delete_lab(username)
    return


@router.get(
    "/{username}/events",
    summary="Get Lab event stream for a user's current operation",
    responses={
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    # FIXME: Not at all sure how to do response model/class for this
)
async def get_user_events(
    username: str,
    context: Context = Depends(context_dependency),
    user_token: str = Depends(user_token_dependency),
) -> AsyncGenerator[ServerSentEvent, None]:
    """Returns the events for the lab of the given user"""
    event_manager = context.event_manager
    # should return EventSourceResponse:
    return event_manager.publish(username)


@router.get(
    "/spawner/v1/user-status",
    responses={
        404: {"description": "Lab not found", "model": ErrorModel},
        403: {"description": "Forbidden", "model": ErrorModel},
    },
    summary="Get status for user",
    response_model=UserData,
)
async def get_user_status(
    context: Context = Depends(context_dependency),
    admin_token: str = Depends(admin_token_dependency),
) -> UserData:
    """Get the pod status for the authenticating user."""
    user = await context.get_user()
    if user is None:
        raise RuntimeError("Cannot get user status without user")
    userdata = context.user_map.get(user.username)
    if userdata is None:
        raise HTTPException(status_code=404, detail="Not found")
    return userdata