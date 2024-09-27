from dataclasses import dataclass
from logging import getLogger
from os import DirEntry, scandir
from re import compile as re_compile, Pattern

logger = getLogger(__name__)


@dataclass(slots=True, frozen=True, kw_only=True)
class IgnoreObject:
    prefixes: tuple[str] = ("sr", "md", "dm-", "loop", "zd", "pmem")
    """A tuple of prefixes for known block devices that we should ignore"""
    pattern: Pattern = re_compile(r"nvme[0-9]+c")
    """An nvme controller device"""
    user_excluded_disks: tuple[str] | tuple = tuple()
    """The caller can provide disk(s) to be excluded
        from fenced. (i.e. boot drives in the head-unit)"""


def should_not_ignore(entry: DirEntry, ignore: IgnoreObject) -> bool:
    """Returns true if the device should NOT be ignored, false otherwise"""
    return all(
        (
            entry.is_symlink(),
            not entry.name.startswith(ignore.prefixes),
            not ignore.pattern.match(entry.name),
            entry.name not in ignore.user_excluded_disks,
        )
    )


def load_disks_from_sys_block(ignore: IgnoreObject) -> dict[str, str]:
    """By the time fenced is called, the OS is booted into multi-user
    mode and has been for a bit. This means we should use sysfs to
    enumerate the disks since using things like libudev, are fickle
    and can have missing devices based on operations done in userspace.
    (i.e. if someone causes a "rescan"). Sysfs does not suffer from
    such scenarios. (Obviously, sysfs can be "updated" if the drive
    catastrophically fails OR someone physically pulls it or someone
    issues an hba reset, etc but there isn't a way to prevent such
    situations in the first place)"""
    disks = {}
    try:
        with scandir("/sys/block") as sdir:
            for disk in filter(lambda x: should_not_ignore(x, ignore), sdir):
                disks[disk.name] = disk.name
    except Exception:
        logger.error("Unhandled exception enumerating disks", exc_info=True)

    return disks


def load_disks_impl(ed: tuple[str] | tuple, use_zpools: bool = False) -> dict[str, str]:
    """
    Return disk(s) to have persistent reservations placed upon.
        `ed`: user supplied disk(s) that will be excluded from having
            persistent reservations placed upon them.
        `use_zpools`: boolean is a NO_OP (for now) and is a placeholder
            for generating a group of disks ONLY associated to a zpool.
    """
    return load_disks_from_sys_block(IgnoreObject(user_excluded_disks=ed))
