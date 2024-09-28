import os.path
from logging import getLogger
from os import DirEntry, scandir
from re import compile as re_compile

logger = getLogger(__name__)

SD_PATTERN = re_compile(r"^sd[a-z]+$")
NVME_PATTERN = re_compile(r"^nvme\d+n\d+$")


def should_not_ignore(entry: DirEntry, ed: tuple[str] | tuple) -> bool:
    """Returns true if the device should NOT be ignored, false otherwise"""
    if not entry.is_symlink() or entry.name in ed:
        return False
    elif SD_PATTERN.match(entry.name) or NVME_PATTERN.match(entry.name):
        return True
    return False


def load_disks_from_sys_block(ed: tuple[str] | tuple) -> dict[str, str]:
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
            for disk in filter(lambda x: should_not_ignore(x, ed), sdir):
                # Only use the disk if it has a surfaced dev
                if os.path.exists(os.path.join(disk.path, 'dev')):
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
    return load_disks_from_sys_block(ed)
