"""Set the configuration, and build a factory for producing test objects."""

from typing import Any, Dict, List

from jupyterlabcontroller.dependencies.config import configuration_dependency
from jupyterlabcontroller.models.v1.consts import lab_statuses, pod_states
from jupyterlabcontroller.models.v1.domain.config import Config
from jupyterlabcontroller.models.v1.domain.labs import LabMap
from jupyterlabcontroller.models.v1.external.userdata import (
    LabSpecification,
    UserData,
    UserInfo,
    UserQuota,
)
from jupyterlabcontroller.utils import memory_string_to_int


def config_config(config_path: str) -> Config:
    """Change the test application configuration.

    Parameters
    ----------
    config_path
      Path to a directory that contains a configuration file
      ``configuration.yaml``, which is the YAML that would usually be
      mounted into the container at ``/etc/nublado/config.yaml``.
    """
    configuration_dependency.set_configuration_path(
        f"{config_path}/config.yaml"
    )
    return configuration_dependency.config()


# Factory to manufacture test objects


class TestObjectFactory:
    _canonicalized: bool = False
    test_objects: Dict[str, Any] = {
        "user_info": [
            {
                "username": "rachel",
                "name": "Rachel (?)",
                "uid": 1101,
                "gid": 1101,
                "groups": [
                    {"name": "rachel", "id": 1101},
                    {"name": "lunatics", "id": 2028},
                    {"name": "mechanics", "id": 2001},
                    {"name": "storytellers", "id": 2021},
                ],
            },
            {
                "username": "wrench",
                "name": "Wrench",
                "uid": 1102,
                "gid": 1102,
                "groups": [
                    {"name": "wrench", "id": 1102},
                    {"name": "jovians", "id": 2010},
                    {"name": "mechanics", "id": 2001},
                ],
            },
            {
                "username": "violet",
                "name": "Violet",
                "uid": 1103,
                "gid": 1103,
                "groups": [
                    {"name": "violet", "id": 1103},
                    {"name": "saturnians", "id": 2011},
                    {"name": "pirates", "id": 2002},
                ],
            },
            {
                "username": "ribbon",
                "name": "Ribbon",
                "uid": 1104,
                "gid": 1104,
                "groups": [
                    {"name": "ribbon", "id": 1104},
                    {"name": "ferrymen", "id": 2023},
                    {"name": "ninjas", "id": 2003},
                ],
            },
        ],
        "quotas": [
            {
                "limits": {
                    "cpu": 4.0,
                    "memory": "12Gi",
                },
                "requests": {"cpu": 1.0, "memory": "3Gi"},
            },
        ],
        "env": [
            {
                "HOME": "/home/ceres",
                "SHELL": "/bin/bash",
            },
        ],
        "user_options": [
            {
                "image": "lighthouse.ceres/library/sketchbook:latest_daily",
                "size": "small",
            },
        ],
        "lab_specification": [],
    }

    def canonicalize(self) -> None:
        if self._canonicalized:
            return
        for idx, x in enumerate(self.test_objects["user_options"]):
            # Glue options and envs into lab specifications
            self.test_objects["lab_specification"].append(
                {
                    "options": x,
                    "env": self.test_objects["env"][
                        idx % len(self.test_objects["env"])
                    ],
                }
            )
            # Set memory to bytes rather than text (e.g. "3KiB" -> 3072)
            for q in self.test_objects["quotas"]:
                for i in ("limits", "requests"):
                    memfld = q[i]["memory"]
                    if type(memfld) is str:
                        q[i]["memory"] = memory_string_to_int(memfld)
        self._canonicalized = True

    @property
    def userinfos(self) -> List[UserInfo]:
        return [UserInfo.parse_obj(x) for x in self.test_objects["user_info"]]

    @property
    def labspecs(self) -> List[LabSpecification]:
        if not self._canonicalized:
            self.canonicalize()
        return [
            LabSpecification.parse_obj(x)
            for x in self.test_objects["lab_specification"]
        ]

    @property
    def quotas(self) -> List[UserQuota]:
        if not self._canonicalized:
            self.canonicalize()
        return [UserQuota.parse_obj(x) for x in self.test_objects["quotas"]]

    @property
    def userdatas(self) -> List[UserData]:
        userdatas: List[UserData] = []
        labspecs = self.labspecs
        quotas = self.quotas
        userinfos = self.userinfos
        for idx, v in enumerate(userinfos):
            userdatas.append(
                UserData.from_components(
                    status=lab_statuses[idx % len(lab_statuses)],
                    pod=pod_states[(idx) % len(pod_states)],
                    user=v,
                    labspec=labspecs[idx % len(labspecs)],
                    quotas=quotas[idx % len(quotas)],
                )
            )
        return userdatas

    @property
    def labmap(self) -> LabMap:
        labmap: LabMap = {}
        for v in self.userdatas:
            n = v.username
            labmap[n] = v
        return labmap


test_object_factory = TestObjectFactory()
