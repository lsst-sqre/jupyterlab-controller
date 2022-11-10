"""Prepull images to nodes, and answer questions about the prepull state.
This requires node inspection and the ability to spawn pods.
"""

import asyncio
from copy import copy
from typing import Any, Dict, List, Optional, Set, Tuple

from aiojobs import Scheduler
from structlog.stdlib import BoundLogger

from ..models.v1.domain.config import Config
from ..models.v1.domain.context import ContextContainer, RequestContext
from ..models.v1.domain.prepuller import (
    ContainerImage,
    DigestToNodeTagImages,
    ExtTag,
    NodeContainers,
    NodeTagImage,
)
from ..models.v1.domain.tag import PartialTag, Tag, TagList, TagType
from ..models.v1.external.lab import UserGroup, UserInfo
from ..models.v1.external.prepuller import (
    DisplayImages,
    Image,
    Node,
    NodeImage,
    NodeImageWithMissing,
    NodeList,
    PendingImages,
    PrepulledImages,
    PrepullerConfig,
    PrepullerContents,
    PrepullerStatus,
)
from ..storage.docker import DockerStorageClient
from ..storage.k8s import (
    Container,
    K8sStorageClient,
    PodSecurityContext,
    PodSpec,
)
from ..utils import get_namespace_prefix


class PrepullerManager:
    def __init__(
        self,
        nublado: ContextContainer,
        context: RequestContext,
    ) -> None:
        self.nublado = nublado
        self.context = context

        self.logger: BoundLogger = self.nublado.logger
        self.k8s_client: K8sStorageClient = self.nublado.k8s_client
        self.config: PrepullerConfig = self.nublado.config.prepuller.config
        self.docker_client: DockerStorageClient = self.nublado.docker_client

    async def get_prepulls(self) -> PrepullerStatus:
        node_images, nodes = await self.get_current_image_and_node_state()

        eligible_nodes: NodeList = [x for x in nodes if x.eligible]

        menu_node_images: Dict[
            str, NodeTagImage
        ] = await self.filter_node_images_to_desired_menu(node_images)

        prepulled: PrepulledImages = []
        pending: PendingImages = []

        for i_name in menu_node_images:
            img: NodeTagImage = menu_node_images[i_name]
            if img.prepulled:
                prepulled.append(
                    NodeImage(
                        path=img.path,
                        name=img.name,
                        digest=img.digest,
                        nodes=await self._nodes_present(img, eligible_nodes),
                    )
                )
            else:
                pending.append(
                    NodeImageWithMissing(
                        path=img.path,
                        name=img.name,
                        digest=img.digest,
                        nodes=await self._nodes_present(img, eligible_nodes),
                        missing=await self._nodes_missing(img, eligible_nodes),
                    )
                )
        images: PrepullerContents = PrepullerContents(
            prepulled=prepulled, pending=pending
        )
        status: PrepullerStatus = PrepullerStatus(
            config=self.config, images=images, nodes=nodes
        )
        self.logger.debug(f"Prepuller status: {status}")
        return status

    async def _nodes_present(
        self, img: NodeTagImage, nodes: NodeList
    ) -> NodeList:
        return [x for x in nodes if x.name in img.nodes]

    async def _nodes_missing(
        self, img: NodeTagImage, nodes: NodeList
    ) -> NodeList:
        return [x for x in nodes if x.name not in img.nodes]

    async def get_menu_images(self) -> DisplayImages:
        node_images, _ = await self.get_current_image_and_node_state()

        menu_node_images: Dict[
            str, NodeTagImage
        ] = await self.filter_node_images_to_desired_menu(node_images)

        available_menu_node_images: Dict[
            str, NodeTagImage
        ] = await self._filter_node_images_by_availability(menu_node_images)

        menu_images: DisplayImages = {}
        for img in available_menu_node_images:
            menu_images[img] = (available_menu_node_images[img]).to_image()
        all_menu: Dict[str, Image] = {}
        for n_img in node_images:
            all_menu[n_img.tag] = n_img.to_image()
        menu_images["all"] = all_menu
        return menu_images

    async def filter_node_images_to_desired_menu(
        self, all_images: List[NodeTagImage]
    ) -> Dict[str, NodeTagImage]:
        menu_images: Dict[str, NodeTagImage] = {}
        for img in all_images:
            # First pass: find recommended tag, put it at top
            if img.tag and img.tag == self.config.recommendedTag:
                menu_images[img.tag] = img
        running_count: Dict[TagType, int] = {}
        tag_count = {
            TagType.RELEASE: self.config.numReleases,
            TagType.WEEKLY: self.config.numWeeklies,
            TagType.DAILY: self.config.numDailies,
        }
        for tag_type in TagType:
            if tag_count.get(tag_type) is None:
                tag_count[tag_type] = 0
            running_count[tag_type] = 0
        for img in all_images:
            assert img.image_type is not None
            tag_type = img.image_type
            running_count[tag_type] += 1
            if running_count[tag_type] > tag_count[tag_type]:
                continue
            if img.tag:
                menu_images[img.tag] = img
        return menu_images

    async def _filter_node_images_by_availability(
        self, menu_node_images: Dict[str, NodeTagImage]
    ) -> Dict[str, NodeTagImage]:
        r: Dict[str, NodeTagImage] = {}
        for k in menu_node_images:
            if menu_node_images[k].prepulled:
                r[k] = menu_node_images[k]
        return r

    async def get_current_image_and_node_state(
        self,
    ) -> Tuple[List[NodeTagImage], NodeList]:
        """This method does all the work that is common to both requesting
        the prepull status and to getting the images that will construct the
        menu.
        """
        self.logger.debug("Listing nodes and their image contents.")
        all_images_by_node = await self.k8s_client.get_image_data()
        self.logger.debug(f"All images on nodes: {all_images_by_node}")
        self.logger.debug("Constructing initial node pool")
        initial_nodes = self._make_nodes_from_image_data(all_images_by_node)
        self.logger.debug(f"Initial node pool: {initial_nodes}")
        self.logger.debug("Constructing image state.")
        image_list = self._construct_current_image_state(all_images_by_node)
        self.logger.debug(f"Image state: {image_list}")
        self.logger.debug("Calculating image prepull status")
        prepulled_images = self._update_prepulled_images(
            initial_nodes, image_list
        )
        self.logger.debug(f"Prepulled images: {prepulled_images}")
        self.logger.debug("Calculating node cache state")
        nodes = self._update_node_cache(initial_nodes, prepulled_images)
        self.logger.debug("Filtering prepulled images to enabled nodes")
        enabled_prepulled_images = self._filter_images_to_enabled_nodes(
            prepulled_images, nodes
        )
        self.logger.debug(
            f"Enabled prepulled images: {enabled_prepulled_images}"
        )
        self.logger.debug(f"Node cache: {nodes}")
        return (enabled_prepulled_images, nodes)

    def _make_nodes_from_image_data(
        self,
        imgdata: NodeContainers,
    ) -> NodeList:
        r: NodeList = [Node(name=n) for n in imgdata.keys()]
        return r

    def _update_prepulled_images(
        self, nodes: NodeList, image_list: List[NodeTagImage]
    ) -> List[NodeTagImage]:
        r: List[NodeTagImage] = []
        eligible = [x for x in nodes if x.eligible]
        nnames = [x.name for x in eligible]
        se: Set[str] = set(nnames)
        for i in image_list:
            sn: Set[str] = set(i.nodes)
            prepulled: bool = True
            if se - sn:
                # Only use eligible nodes to determine prepulled status
                prepulled = False
            c = copy(i)
            c.prepulled = prepulled
            r.append(c)
        return r

    def _update_node_cache(
        self, nodes: NodeList, image_list: List[NodeTagImage]
    ) -> NodeList:
        r: NodeList = []
        tagobjs: List[Tag]
        dmap: Dict[str, Dict[str, Any]] = {}
        for i in image_list:
            img = i.to_image()
            if img.digest not in dmap:
                dmap[img.digest] = {}
            dmap[img.digest]["img"] = img
            dmap[img.digest]["nodes"] = i.nodes
        for node in nodes:
            for i in image_list:
                dg = i.digest
                nl = dmap[dg]["nodes"]
                if node.name in nl:
                    node.cached.append(dmap[dg]["img"])
            r.append(node)
        return r

    def _filter_images_to_enabled_nodes(
        self,
        images: List[NodeTagImage],
        nodes: NodeList,
    ) -> List[NodeTagImage]:
        eligible_nodes = [x.name for x in nodes if x.eligible]
        filtered_images: List[NodeTagImage] = []
        for img in images:
            filtered = NodeTagImage(
                path=img.path,
                name=img.name,
                digest=img.digest,
                tags=copy(img.tags),
                size=img.size,
                prepulled=img.prepulled,
                tag=img.tag,
                nodes=[x for x in img.nodes if x in eligible_nodes],
                known_alias_tags=copy(img.known_alias_tags),
                tagobjs=copy(img.tagobjs),
                image_type=img.image_type,
            )
            filtered_images.append(filtered)
        return filtered_images

    def _construct_current_image_state(
        self,
        all_images_by_node: NodeContainers,
    ) -> List[NodeTagImage]:
        """Return annotated images representing the state of valid images
        across nodes.
        """

        # Filter images by config

        filtered_images = self._filter_images_by_config(all_images_by_node)

        # Convert to extended Tags.  We will still have duplicates.
        exttags: List[ExtTag] = self._get_exttags_from_images(filtered_images)

        # Filter by cycle

        cycletags: List[ExtTag] = self._filter_exttags_by_cycle(exttags)

        # Deduplicate and convert to NodeTagImages.

        node_images: List[NodeTagImage] = self._get_images_from_exttags(
            cycletags
        )
        self.logger.debug(f"Filtered, deduplicated images: {node_images}")
        return node_images

    def _get_images_from_exttags(
        self,
        exttags: List[ExtTag],
    ) -> List[NodeTagImage]:
        dmap: DigestToNodeTagImages = {}
        for exttag in exttags:
            digest = exttag.digest
            if digest is None or digest == "":
                # This is completely normal; only one pseudo-tag is going to
                # have a digest.
                continue
            img = NodeTagImage(
                path=self._extract_path_from_image_ref(exttag.image_ref),
                digest=digest,
                name=exttag.display_name,
                size=exttag.size,
                nodes=[exttag.node],
                known_alias_tags=exttag.config_aliases,
                tags={exttag.tag: exttag.display_name},
                prepulled=False,
            )

            if digest not in dmap:
                self.logger.debug(
                    f"Adding {digest} as {img.path}:{exttag.tag}"
                )
                dmap[digest] = img
            else:
                extant_image = dmap[digest]
                if img.path != extant_image.path:
                    self.logger.warning(
                        f"Image {digest} found as {img.path} "
                        + f"and also {extant_image.path}."
                    )
                    continue
                extant_image.tags.update(img.tags)
                if (
                    exttag.node is not None
                    and exttag.node not in extant_image.nodes
                ):
                    extant_image.nodes.append(exttag.node)
                if exttag.config_aliases is not None:
                    for alias in exttag.config_aliases:
                        if alias not in extant_image.known_alias_tags:
                            extant_image.known_alias_tags.append(alias)
        for digest in dmap:
            self.logger.debug(f"Img before tag consolidation: {dmap[digest]}")
            dmap[digest].consolidate_tags(
                recommended=self.config.recommendedTag
            )
            self.logger.debug(f"Img after tag consolidation: {dmap[digest]}")
            self.logger.debug(f"Images hash: {dmap}")
        return list(dmap.values())

    def _get_exttags_from_images(self, nc: NodeContainers) -> List[ExtTag]:
        r: List[ExtTag] = []
        for node in nc:
            ctrs = nc[node]
            for ctr in ctrs:
                t = self._make_exttags_from_ctr(ctr, node)
                r.extend(t)
        return r

    def _make_exttags_from_ctr(
        self,
        ctr: ContainerImage,
        node: str,
    ) -> List[ExtTag]:
        r: List[ExtTag] = []
        digest: str = ""
        for c in ctr.names:
            # Extract the digest, making sure we don't have conflicting
            # digests.
            if "@sha256:" in c:
                _nd = c.split("@")[-1]
                if not digest:
                    digest = _nd
                assert digest == _nd, f"{c} has multiple digests"
            for c in ctr.names:
                # Start over and do it with tags.  Skip the digest.
                # That does mean there's no way to get untagged images out of
                # the config unless it's a pin.
                if "@sha256:" in c:
                    continue
                tag = c.split(":")[-1]
                assert self.config.aliasTags is not None
                config_aliases: List[str] = self.config.aliasTags
                partial = PartialTag.parse_tag(tag=tag)
                if partial.display_name == tag:
                    partial.display_name = PartialTag.prettify_tag(tag=tag)
                tagobj = ExtTag(
                    tag=tag,
                    image_ref=c,
                    digest=digest,
                    node=node,
                    config_aliases=config_aliases,
                    image_type=partial.image_type,
                    display_name=partial.display_name,
                    semantic_version=partial.semantic_version,
                    cycle=partial.cycle,
                    size=ctr.size_bytes,
                )
                r.append(tagobj)
        return r

    def _node_containers_to_images(
        self, nc: NodeContainers
    ) -> List[NodeTagImage]:
        r: List[NodeTagImage] = []
        for node in nc:
            for ctr in nc[node]:
                img = self.image_from_container(ctr, node)
                r.append(img)
        return r

    def image_from_container(
        self, ctr: ContainerImage, node: str
    ) -> NodeTagImage:
        path = self._extract_path_from_v1_container(ctr)
        size = ctr.size_bytes
        digest = ""
        tagobjs: List[Tag] = []
        for c in ctr.names:
            # Extract the digest, making sure we don't have conflicting
            # digests.
            if "@sha256:" in c:
                _nd = c.split("@")[-1]
                if not digest:
                    digest = _nd
                assert digest == _nd, f"Image at {path} has multiple digests"
        self.logger.debug(f"Found digest: {digest}")
        for c in ctr.names:
            # Start over and do it with tags.
            if "@sha256:" in c:
                continue
            tag = c.split(":")[-1]
            tagobj = Tag.from_tag(tag=tag, image_ref=c, digest=digest)
            tagobjs.append(tagobj)
            tags: Dict[str, str] = {}
        tagobjlist = TagList(all_tags=tagobjs)
        for t in tagobjs:
            tags[t.tag] = t.display_name
            r = NodeTagImage(
                digest=digest,
                path=path,
                tags=tags,
                tagobjs=tagobjlist,
                size=size,
                prepulled=False,
                name="",  # About to be set from instance method
                known_alias_tags=[],
                nodes=[],
            )
        return r

    def _extract_image_name(self) -> str:
        c = self.config
        if c.gar is not None:
            return c.gar.image
        if c.docker is not None:
            return c.docker.repository.split("/")[-1]
        assert False, f"Config {c} sets neither 'gar' nor 'docker'!"

    def _extract_path_from_v1_container(self, c: ContainerImage) -> str:
        return self._extract_path_from_image_ref(c.names[0])

    def _extract_path_from_image_ref(self, tname: str) -> str:
        # Remove the specifier from either a digest or a tagged image
        if "@sha256:" in tname:
            # Everything before the '@'
            untagged = tname.split("@")[0]
        else:
            # Everything before the last ':'
            untagged = ":".join(tname.split(":")[:-1])
        return untagged

    def _filter_images_by_config(
        self,
        images: NodeContainers,
    ) -> NodeContainers:
        r: NodeContainers = {}

        name = self._extract_image_name()
        self.logger.debug(f"Desired image name: {name}")
        for node in images:
            for c in images[node]:
                path = self._extract_path_from_v1_container(c)
                img_name = path.split("/")[-1]
                if img_name == name:
                    self.logger.debug(f"Adding matching image: {img_name}")
                    if node not in r:
                        r[node] = []
                    t = copy(c)
                    r[node].append(t)
        return r

    def _filter_exttags_by_cycle(self, exttags: List[ExtTag]) -> List[ExtTag]:
        if self.config.cycle is None:
            return exttags
        return [t for t in exttags if t.cycle == self.config.cycle]


class PrepullExecutor(PrepullerManager):
    """This extends PrepullerManager with the functionality to actually
    create prepulled pods as needed.

    Since we won't be called from a handler, we need to build our own
    nublado configuration context and request context.  We will have a
    config object already.  (The test for whether those contexts already
    exists is here to simplify testing, by allowing reuse of extant
    test fixtures.)

    The only piece of the request context we need is the uid, so for
    now we're just going to hardcode that to 1000 (which is ``lsst_lcl``
    in a sciplat-lab pod).

    It really doesn't matter: the only action we take is sleeping for five
    seconds, so not being in NSS doesn't make a difference, and "any non-zero
    uid" will work just fine.
    """

    stopping: bool = False
    schedulers: Dict[str, Scheduler] = {}

    def __init__(
        self,
        config: Optional[Config] = None,
        nublado: Optional[ContextContainer] = None,
        context: Optional[RequestContext] = None,
    ) -> None:
        if nublado is None or context is None:
            assert config is not None, "Config must be specified"
            nublado = ContextContainer.initialize(config=config)
            context = RequestContext(
                token="token-of-affection",
                namespace=get_namespace_prefix(),
                user=UserInfo(
                    username="prepuller",
                    name="Prepuller User",
                    uid=1000,
                    gid=1000,
                    groups=[
                        UserGroup(
                            name="prepuller",
                            id=1000,
                        )
                    ],
                ),
            )
        assert (
            nublado is not None and context is not None
        ), "Nublado context and request context must be specified"
        super().__init__(nublado=nublado, context=context)

    async def run(self) -> None:
        """
        Loop until we're told to stop.
        """

        self.logger.info("Starting prepull executor.")
        await self.startup()
        try:
            while not self.stopping:
                await self.prepull_images()
                await self.idle()
        except asyncio.CancelledError:
            self.logger.info("Prepull executor interrupted.")
        self.logger.info("Shutting down prepull executor.")
        await self.shutdown()

    async def idle(self) -> None:
        await asyncio.sleep(self.nublado.config.prepuller.pollInterval)

    async def startup(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopping = True

    async def shutdown(self) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close any prepull schedulers."""
        if self.schedulers:
            scheduler = Scheduler(
                close_timeout=self.nublado.config.kubernetes.request_timeout
            )
            for image in self.schedulers:
                self.logger.warning(f"Terminating scheduler for {image}")
                await scheduler.spawn(self.schedulers[image].close())
            await scheduler.close()
            for image in list(self.schedulers.keys()):
                del self.schedulers[image]

    async def create_prepuller_pod_spec(
        self, image: str, node: str
    ) -> PodSpec:
        shortname = image.split("/")[-1]
        return PodSpec(
            containers=[
                Container(
                    name=f"prepull-{shortname}",
                    command=["/bin/sleep", "5"],
                    image=image,
                    security_context=PodSecurityContext(
                        run_as_non_root=True,
                        run_as_user=self.context.user.uid,
                    ),
                    working_dir="/tmp",
                )
            ],
            node_name=node,
        )

    async def prepull_images(self) -> None:
        """This is the method to identify everything that needs pulling, and
        spawns pods with those images on the node that needs them.
        """

        status: PrepullerStatus = await self.get_prepulls()

        pending = status.images.pending

        required_pulls: Dict[str, List[str]] = {}
        for img in pending:
            for i in img.missing:
                if i.eligible:
                    if img.path not in required_pulls:
                        required_pulls[img.path] = []
                    required_pulls[img.path].append(i.name)
        self.logger.debug(f"Required pulls by node: {required_pulls}")
        timeout = self.nublado.config.prepuller.pullTimeout
        # Parallelize across nodes but not across images
        for image in required_pulls:
            if image in self.schedulers:
                self.logger.warning(
                    f"Scheduler for image {image} already exists.  Presuming "
                    "earlier pull still in progress."
                )
                continue
            scheduler = Scheduler(close_timeout=timeout)
            self.schedulers[image] = scheduler
            tag = image.split(":")[1]
            for node in required_pulls[image]:
                await scheduler.spawn(
                    self.nublado.k8s_client.create_pod(
                        name=f"prepull-{tag}",
                        namespace=get_namespace_prefix(),
                        pod=await self.create_prepuller_pod_spec(
                            image=image,
                            node=node,
                        ),
                    )
                )
            self.logger.debug(
                f"Waiting up to {timeout}s for prepuller pods {tag}."
            )
            await scheduler.close()
            del self.schedulers[image]