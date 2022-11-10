"""Produce a test object factory"""

from __future__ import annotations

import json
from copy import copy
from typing import Any, Dict, List

from jupyterlabcontroller.models.v1.consts import lab_statuses, pod_states
from jupyterlabcontroller.models.v1.domain.lab import UserMap
from jupyterlabcontroller.models.v1.domain.prepuller import NodeContainers
from jupyterlabcontroller.models.v1.external.lab import (
    LabSpecification,
    UserData,
    UserInfo,
    UserQuota,
)
from jupyterlabcontroller.storage.k8s import ContainerImage, ContainerImageList
from jupyterlabcontroller.utils import memory_string_to_int

# Factory to manufacture test objects


class TestObjectFactory:
    _filename: str = ""
    _canonicalized: bool = False
    test_objects: Dict[str, Any] = {}

    def initialize_from_file(self, filename: str) -> None:
        if filename and filename != self._filename:
            with open(filename) as f:
                self.test_objects = json.load(f)
                self._filename = filename
            self.canonicalize()

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
            # Make node contents into ContainerImageList
            for node in self.test_objects["node_contents"]:
                nc = self.test_objects["node_contents"][node]
                clist: ContainerImageList = []
                for img in nc:
                    clist.append(
                        ContainerImage(
                            names=copy(img["names"]),
                            size_bytes=img["sizeBytes"],
                        )
                    )
                self.test_objects["node_contents"][node] = clist
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
                    quota=quotas[idx % len(quotas)],
                )
            )
        return userdatas

    @property
    def usermap(self) -> UserMap:
        usermap: UserMap = {}
        for v in self.userdatas:
            n = v.username
            usermap[n] = v
        return usermap

    @property
    def nodecontents(self) -> NodeContainers:
        if not self._canonicalized:
            self.canonicalize()
        return self.test_objects["node_contents"]


test_object_factory = TestObjectFactory()