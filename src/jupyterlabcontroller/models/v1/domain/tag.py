"""Classes to hold all the semantic data and metadata we can extract from a
tag in the format specified by https://sqr-059.lsst.io.  Mostly simplified
from cachemachine's implementation."""

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Match, Optional, Tuple

from semver import VersionInfo

from ..external.prepuller import Image


class TagType(Enum):
    """Enum specifying different tag types for Rubin Science Platform Lab
    images, and an Exception for attempted comparison between
    incomparable types.

    These are listed in order of priority to make sorting images easier.
    """

    RELEASE = auto()
    WEEKLY = auto()
    DAILY = auto()
    RELEASE_CANDIDATE = auto()
    EXPERIMENTAL = auto()
    ALIAS = auto()
    UNKNOWN = auto()


DOCKER_DEFAULT_TAG = "latest"

# Build the raw strings for tags and tag components, which can then be
# mixed and matched to some degree.  Cuts down a little on the complexity of
# the TAGTYPE_REGEXPS tuple and prevents some duplication.  We will use named
# group matches to make understanding the tag data easier.
TAG: Dict[str, str] = {
    # r22_0_1
    "release": r"r(?P<major>\d+)_(?P<minor>\d+)_(?P<patch>\d+)",
    # r23_0_0_rc1
    "rc": r"r(?P<major>\d+)_(?P<minor>\d+)_(?P<patch>\d+)_rc(?P<pre>\d+)",
    # w_2021_13
    "weekly": r"w_(?P<year>\d+)_(?P<week>\d+)",
    # d_2021_05_13
    "daily": r"d_(?P<year>\d+)_(?P<month>\d+)_(?P<day>\d+)",
    # exp_flattened_build
    "experimental": r"(?:exp)",
    # c0020.002
    "cycle": r"_(?P<ctag>c|csal)(?P<cycle>\d+)\.(?P<cbuild>\d+)",
    # _whatever_your_little_heart_desires
    "rest": r"_(?P<rest>.*)",
}

# This is the heart of the parser: it's an ordered list of tuples, each of
# which contains a tag type followed by a regular expression defining
# something that matches that type, with named capture groups.
#
# Note that this is matched top to bottom.  In particular, the release
# candidate images must precede the release images, because an RC candidate
# could be a release image with non-empty "rest".
#
TAGTYPE_REGEXPS: List[Tuple[TagType, re.Pattern]] = [
    # r23_0_0_rc1_c0020.001_20210513
    (
        TagType.RELEASE_CANDIDATE,
        re.compile(TAG["rc"] + TAG["cycle"] + TAG["rest"] + r"$"),
    ),
    # r23_0_0_rc1_c0020.001
    (
        TagType.RELEASE_CANDIDATE,
        re.compile(TAG["rc"] + TAG["cycle"] + r"$"),
    ),
    # r23_0_0_rc1_20210513
    (
        TagType.RELEASE_CANDIDATE,
        re.compile(TAG["rc"] + TAG["rest"] + r"$"),
    ),
    # r23_0_0_rc1
    (TagType.RELEASE_CANDIDATE, re.compile(TAG["rc"] + r"$")),
    # r22_0_1_c0019.001_20210513
    (
        TagType.RELEASE,
        re.compile(TAG["release"] + TAG["cycle"] + TAG["rest"] + r"$"),
    ),
    # r22_0_1_c0019.001
    (TagType.RELEASE, re.compile(TAG["release"] + TAG["cycle"] + r"$")),
    # r22_0_1_20210513
    (TagType.RELEASE, re.compile(TAG["release"] + TAG["rest"] + r"$")),
    # r22_0_1
    (TagType.RELEASE, re.compile(TAG["release"] + r"$")),
    # r170 (obsolete) (no new ones, no additional parts)
    (TagType.RELEASE, re.compile(r"r(?P<major>\d\d)(?P<minor>\d)$")),
    # w_2021_13_c0020.001_20210513
    (
        TagType.WEEKLY,
        re.compile(TAG["weekly"] + TAG["cycle"] + TAG["rest"] + r"$"),
    ),
    # w_2021_13_c0020.001
    (TagType.WEEKLY, re.compile(TAG["weekly"] + TAG["cycle"] + r"$")),
    # w_2021_13_20210513
    (TagType.WEEKLY, re.compile(TAG["weekly"] + TAG["rest"] + r"$")),
    # w_2021_13
    (TagType.WEEKLY, re.compile(TAG["weekly"] + r"$")),
    # d_2021_05_13_c0019.001_20210513
    (
        TagType.DAILY,
        re.compile(TAG["daily"] + TAG["cycle"] + TAG["rest"] + r"$"),
    ),
    # d_2021_05_13_c0019.001
    (TagType.DAILY, re.compile(TAG["daily"] + TAG["cycle"] + r"$")),
    # d_2021_05_13_20210513
    (TagType.DAILY, re.compile(TAG["daily"] + TAG["rest"] + r"$")),
    # d_2021_05_13
    (TagType.DAILY, re.compile(TAG["daily"] + r"$")),
    # exp_w_2021_05_13_nosudo
    (
        TagType.EXPERIMENTAL,
        re.compile(TAG["experimental"] + TAG["rest"] + r"$"),
    ),
]


@dataclass
class PartialTag:
    """The primary method of construction of a PartialTag is the
    parse_tag classmethod.  The PartialTag holds the data that comes
    from the tag, but not the associated data such as image_digest or
    image_ref.  It does construct the provisional display name, but does not
    know about alias tags."""

    tag: str
    """This is the tag on a given image.  We assume there is one and
    only one Docker path for all our tags.  If we need access to
    multiple image names, repositories, or hosts, we will need a
    different strategy.

    example: w_2021_22
    """

    image_type: TagType
    """Rubin-specific RSP Lab image type.

    example: TagType.WEEKLY
    """

    display_name: str
    """Human-readable display name corresponding to a tag.

    example: Weekly 2021_22
    """

    semantic_version: Optional[VersionInfo]
    """Semantic version constructed from a tag.  Only extant for Daily,
    Weekly, Release, and Release Candidate image types.  Only meaningful for
    comparison within a type.

    example: VersionInfo(2021,22,0)
    """

    cycle: Optional[int]
    """XML Cycle for a given image.  Only used in T&S builds.

    example: 20
    """

    # Required for SemanticVersion
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def parse_tag(
        cls,
        tag: str,
    ) -> "PartialTag":
        if not tag:
            tag = DOCKER_DEFAULT_TAG  # This is a Docker convention
        for (tagtype, regexp) in TAGTYPE_REGEXPS:
            match = re.compile(regexp).match(tag)
            if not match:
                continue
            display_name, semver, cycle = PartialTag.extract_metadata(
                match=match, tag=tag, tagtype=tagtype
            )
            return cls(
                tag=tag,
                image_type=tagtype,
                display_name=display_name,
                semantic_version=semver,
                cycle=cycle,
            )
        # Didn't find any matches
        return cls(
            tag=tag,
            image_type=TagType.UNKNOWN,
            display_name=tag,
            semantic_version=None,
            cycle=None,
        )

    """Some static methods that are used, ultimately, by parse_tag.
    """

    @staticmethod
    def prettify_tag(tag: str) -> str:
        """Little convenience wrapper for turning
        (possibly-underscore-separated) tags into prettier space-separated
        title case."""
        return tag.replace("_", " ").title()

    @staticmethod
    def extract_metadata(
        match: Match,
        tag: str,
        tagtype: TagType,
    ) -> Tuple[str, Optional[VersionInfo], Optional[int]]:
        """Return a display name, semantic version (optional), and cycle
        (optional) from match, tag, and type."""
        md = match.groupdict()
        name = tag
        semver = None
        ctag = md.get("ctag")
        cycle = md.get("cycle")
        cbuild = md.get("cbuild")
        cycle_int = None
        rest = md.get("rest")
        # We have our defaults.  The rest is optimistically seeing if we can
        # do better
        if tagtype == TagType.UNKNOWN:
            # We can't do anything better, but we really shouldn't be
            # extracting from an unknown type.
            pass
        elif tagtype == TagType.EXPERIMENTAL:
            # This one is slightly complicated.  Because of the way the build
            # process works, our tag likely looks like exp_<other-legal-tag>.
            # So we try that hypothesis.  If that's not how the tag is
            # constructed, nname will just come back as everything
            # after "exp_".
            if rest is not None:
                # it actually never will be None if the regexp matched, but
                # mypy doesn't know that
                temp_ptag = PartialTag.parse_tag(rest)
                # We only care about the display name, not any other fields.
                name = f"Experimental {temp_ptag.display_name}"
        else:
            # Everything else does get an actual semantic version
            build = PartialTag.trailing_parts_to_semver_build_component(
                cycle, cbuild, ctag, rest
            )
            typename = PartialTag.prettify_tag(tagtype.name)
            restname = name[2:]
            if (
                tagtype == TagType.RELEASE
                or tagtype == TagType.RELEASE_CANDIDATE
            ):
                # This is bulky because we don't want to raise an error here
                # if we cannot extract a required field; instead we let the
                # field be None, and then the semantic version construction
                # fails later.  That's OK too, because we try that in a
                # try/expect block and return None if we can't construct
                # a version.  In *that* case we have a tag without semantic
                # version information--which is allowable.
                major = PartialTag.maybe_int(md.get("major"))
                minor = PartialTag.maybe_int(md.get("minor"))
                patch = PartialTag.maybe_int(
                    md.get("patch", "0")
                )  # If omitted, it's zero
                restname = f"r{major}.{minor}.{patch}"
                pre = md.get("pre")
                if pre:
                    pre = f"rc{pre}"
                    restname += f"-{pre}"
            else:  # tagtype is weekly or daily
                year = md.get("year")
                month = md.get("month")
                week = md.get("week")
                day = md.get("day")
                major = PartialTag.maybe_int(year)
                if tagtype == TagType.WEEKLY:
                    minor = PartialTag.maybe_int(week)
                    patch = 0
                    restname = (
                        f"{year}_{week}"  # preserve initial string format
                    )
                else:
                    minor = PartialTag.maybe_int(md.get("month"))
                    patch = PartialTag.maybe_int(md.get("day"))
                    restname = (
                        f"{year}_{month}_{day}"  # preserve string format
                    )
                pre = None
            try:
                semver = VersionInfo(
                    major=major,
                    minor=minor,
                    patch=patch,
                    prerelease=pre,
                    build=build,
                )
            except TypeError:
                pass
            name = f"{typename} {restname}"  # Glue together display name.
            if cycle:
                name += f" (SAL Cycle {cycle}, Build {cbuild})"
            if rest:
                name += f" [{rest}]"
            cycle_int = PartialTag.maybe_int(cycle)
        return (name, semver, cycle_int)

    @staticmethod
    def maybe_int(n: Optional[str]) -> Optional[int]:
        if n is None:
            return None
        return int(n)

    @staticmethod
    def trailing_parts_to_semver_build_component(
        cycle: Optional[str],
        cbuild: Optional[str],
        ctag: Optional[str],  # if present, either 'c' or 'csal'
        rest: Optional[str] = None,
    ) -> Optional[str]:
        """This takes care of massaging the cycle components, and 'rest', into
        a semver-compatible buildstring, which is dot-separated and can only
        contain alphanumerics.  See SQR-059 for how it's used.
        (https://github.com/lsst-sqre/sqr-059)
        """
        if cycle:
            if rest:
                # Cycle must always precede rest
                rest = f"{ctag}{cycle}.{cbuild}_{rest}"
            else:
                rest = f"{ctag}{cycle}.{cbuild}"
        # We're done with cycle components now.
        if not rest:
            return None
        rest = rest.replace("_", ".")
        pat = re.compile(r"[^\w|\.]+")  # Identify all non alphanum, non-dots
        # Throw away all of those after turning underscores to dots.
        rest = pat.sub("", rest)
        if not rest:  # if we are left with an empty string, return None
            return None
        return rest

    def compare(self, other: "PartialTag") -> int:
        """This is modelled after semver.compare, but raises an exception
        if the images do not have the same image_type."""
        if self.image_type != other.image_type:
            raise IncomparableImageTypesError(
                f"Tag '{self.tag}' of type {self.image_type} cannot be "
                + f"compared to '{other.tag}' of type {other.image_type}."
            )
        # The easy case: we have a type with a semantic_version attribute.
        # Use it.
        if (
            self.semantic_version is not None
            and other.semantic_version is not None
        ):
            return self.semantic_version.compare(other.semantic_version)
        # Otherwise, all we can do is sort lexigraphically by tag.
        # Experimentals can be sorted only by tag.
        if self.tag == other.tag:
            return 0
        if self.tag < other.tag:
            return -1
        return 1

    """Implement comparison operators."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PartialTag):
            return NotImplemented
        return self.compare(other) == 0

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __gt__(self, other: "PartialTag") -> bool:
        return self.compare(other) == 1

    def __le__(self, other: "PartialTag") -> bool:
        return not self.__gt__(other)

    def __lt__(self, other: "PartialTag") -> bool:
        return self.compare(other) == -1

    def __ge__(self, other: "PartialTag") -> bool:
        return not self.__lt__(other)


@dataclass
class Tag(PartialTag):
    """The primary method of Tag construction
    is the from_tag classmethod.  The Tag holds all the metadata
    encoded within a particular tag (in its base class) and also additional
    metadata known and/or calculated via outside sources, such as the
    image digest, whether the image is an alias, and the image reference.
    """

    image_ref: str
    """This is the Docker reference for this particular image.  It's not
    actually used within this class, but it's useful as general image
    metadata, since it's required to pull the image.

    example: index.docker.io/lsstsqre/sciplat-lab:w_2021_22
    """

    digest: Optional[str]
    """Image digest for a particular image.

    example: "sha256:419c4b7e14603711b25fa9e0569460a753c4b2449fe275bb5f89743b01794a30"  # noqa: E501
    """

    # We use a classmethod here rather than just allowing specification of
    # the fields because we generally want to derive most of our attributes.
    @classmethod
    def from_tag(
        cls,
        tag: str,
        image_ref: str = "",
        alias_tags: List[str] = [],
        override_name: str = "",
        digest: Optional[str] = None,
        override_cycle: Optional[int] = None,
    ) -> "Tag":
        """Create a Tag object from a tag and a list of alias tags.
        Allow overriding name rather than generating one, and allow an
        optional digest parameter."""
        partial_tag = PartialTag.parse_tag(tag)
        image_type = partial_tag.image_type
        display_name = partial_tag.display_name
        cycle = partial_tag.cycle
        # Here's where we glue in the alias knowledge.  Note that we just
        # special-case "latest" and "latest_<anything>"
        if tag in alias_tags or tag == "latest" or tag.startswith("latest_"):
            image_type = TagType.ALIAS
            display_name = PartialTag.prettify_tag(tag)
        # And here we override the name if appropriate.
        if override_name:
            display_name = override_name
        # Override cycle if appropriate
        if override_cycle:
            cycle = override_cycle
        return cls(
            tag=tag,
            image_ref=image_ref,
            digest=digest,
            image_type=image_type,
            display_name=display_name,
            semantic_version=partial_tag.semantic_version,
            cycle=cycle,
        )

    def is_recognized(self) -> bool:
        """Only return true if the image is a known type that is not known
        to be an alias.  It's possible that we also want to exclude
        experimental images.
        """
        img_type = self.image_type
        unrecognized = [TagType.UNKNOWN, TagType.ALIAS]
        if img_type in unrecognized:
            return False
        return True


@dataclass
class TagList:
    """This is a class to hold tag objects and return sorted lists of them
    for construction of the image menu.
    """

    all_tags: List[Tag] = field(default_factory=list)

    def sort_all_tags(self) -> None:
        """This sorts the ``all_tags`` field according to the ordering of
        the TagType enum."""
        new_tags: Dict[TagType, List[Tag]] = {}
        for tag_type in TagType:  # Initialize the dict, relying on the fact
            # that dicts are insertion-ordered in Python 3.6+ (we require
            # 3.10)
            new_tags[tag_type] = []
        for tag in self.all_tags:
            new_tags[tag.image_type].append(tag)
        # Now sort the tags within each type in reverse lexical order.  This
        # will sort them with most recent first, because of the tag type
        # definitions.
        for tag_type in TagType:
            new_tags[tag.image_type].sort(reverse=True)
        # And flatten it out into a homogeneous list.
        flat_tags: List[Tag] = []
        for k in new_tags:
            if new_tags[k] is not None:
                flat_tags.extend(new_tags[k])
        self.all_tags = flat_tags

    def sorted_images(self, img_type: TagType, count: int = 0) -> List[Image]:
        """This returns a sorted list of images for a given type, highest
        version (and thus most recent) at the top.  The count
        parameter specifies how many images should be in the list; leaving it
        at its default of 0 will return the entire list.
        """
        imgs = sorted(
            [
                t
                for t in self.all_tags
                if (t is not None and img_type == t.image_type)
            ],
            reverse=True,
        )
        if count is not None:
            imgs = imgs[:count]
        taglist = TagList(all_tags=imgs)
        return taglist.to_dockerimagelist()

    def to_dockerimagelist(self, name_is_tag: bool = False) -> List[Image]:
        image_list: List[Image] = []
        nonempty_tags = [t for t in self.all_tags.copy() if t is not None]
        for t in nonempty_tags:
            image_list.append(
                Image(
                    path=t.image_ref,
                    digest=(t.digest or ""),
                    name=(
                        lambda name_is_tag: t.tag
                        if name_is_tag
                        else t.display_name
                    )(name_is_tag),
                )
            )
        return image_list


class IncomparableImageTypesError(Exception):
    pass
