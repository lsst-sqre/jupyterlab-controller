from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, TypeAlias, Union

import yaml
from pydantic import BaseModel, validator
from structlog.stdlib import BoundLogger

from ..external.prepuller import Config as ExternalPrepullerConfig
from ..external.userdata import UserEnv

#
# Safir
#


class SafirConfig(BaseModel):
    name: str
    profile: str
    logger_name: str
    log_level: str

    @validator("profile")
    def validate_profile(cls, v: str) -> str:
        assert v in ("production", "development")
        return v


#
# K8s
#


class K8sConfig(BaseModel):
    request_timeout: int = 60


#
# Lab
#

# https://www.d20srd.org/srd/combat/movementPositionAndDistance.htm#bigandLittleCreaturesInCombat
_srdsizes = (
    "fine",
    "diminutive",
    "tiny",
    "small",
    "medium",
    "large",
    "huge",
    "gargantuan",
    "colossal",
)


class LabSizeDefinition(BaseModel):
    cpu: float = 0.5
    memory: Union[int, str] = "1536MiB"


LabSizeDefinitions: TypeAlias = Dict[str, LabSizeDefinition]


class LabSecurityContext(BaseModel):
    runAsUser: int = 1000
    runAsNonRootUser: bool = True
    allowPrivilegeEscalation: bool = False


class LabInitContainer(BaseModel):
    name: str
    image: str
    securityContext: LabSecurityContext


LabInitContainers: TypeAlias = List[LabInitContainer]

# The quota is just the sum of many sizes, effectively
LabQuota = LabSizeDefinition


class LabNFSDefinition(BaseModel):
    path: str
    server: str


class LabVolume(BaseModel):
    name: str
    nfs: LabNFSDefinition


LabVolumes: TypeAlias = List[LabVolume]


class LabVolumeMount(BaseModel):
    name: str
    mountPath: str

    @validator("mountPath")
    def validate_lab_mount_path(cls, v: str) -> str:
        assert v.startswith("/")
        return v


LabVolumeMounts: TypeAlias = List[LabVolumeMount]


class LabFormRestriction(BaseModel):
    type: str
    value: str
    groups: Optional[List[str]] = None

    @validator("type")
    def validate_form_type(cls, v: str) -> str:
        assert v in ("size", "image", "tag")
        return v

    @validator("value")
    def validate_form_value(cls, v: str) -> str:
        _ = re.compile(v)  # Will throw an exception if it's not a valid RE
        return v


LabFormRestrictionList: TypeAlias = List[LabFormRestriction]


class LabForm(BaseModel):
    restrictions: LabFormRestrictionList


class LabFile(BaseModel):
    name: str
    mountPath: str
    contents: str
    modify: bool = False

    @validator("mountPath")
    def validate_lab_mount_path(cls, v: str) -> str:
        assert v.startswith("/")
        return v


LabFiles: TypeAlias = List[LabFile]


class LabConfig(BaseModel):
    sizes: LabSizeDefinitions
    form: LabForm
    env: UserEnv = {}
    files: LabFiles = []
    volumes: LabVolumes = []
    volume_mounts: LabVolumeMounts = []
    initcontainers: LabInitContainers = []
    quota: Optional[LabQuota] = None

    @validator("sizes")
    def validate_lab_sizes(
        cls, v: Dict[str, LabSizeDefinition]
    ) -> Dict[str, LabSizeDefinition]:
        for sz_name in v.keys():
            assert sz_name in _srdsizes
        return v


#
# Prepuller is the external prepuller Config model
#


class PrepullerConfig(BaseModel):
    config: ExternalPrepullerConfig


#
# Form
#


FormData: TypeAlias = Dict[str, str]


class FormsConfig(BaseModel):
    forms: FormData

    @validator("forms")
    def validate_form(cls, v: FormData) -> FormData:
        assert "default" in v.keys()
        return v


#
# Config
#


class Config(BaseModel):
    safir: SafirConfig
    kubernetes: K8sConfig
    lab: LabConfig
    prepuller: PrepullerConfig
    form: FormsConfig
    path: Optional[str] = None

    @classmethod
    def from_file(
        cls,
        filename: str,
        logger: BoundLogger,
    ) -> Config:
        config_obj: Dict[Any, Any] = yaml.safe_load(filename)
        with open(filename) as f:
            config_obj = yaml.safe_load(f)
            # In general the YAML might have configuration for other
            # objects than the controller in it.
            r = Config.parse_obj(config_obj["controller"])
            r.path = filename
            return r