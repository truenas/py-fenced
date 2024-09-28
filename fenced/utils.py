from logging import getLogger
from os import DirEntry, scandir
from re import compile as re_compile

logger = getLogger(__name__)

SD_PATTERN = re_compile(r"^sd[a-z]+$")
NVME_PATTERN = re_compile(r"^nvme\d+n\d+$")


def should_not_ignore(entry: DirEntry, ed: tuple[str] | tuple) -> bool:
    """Returns true if the device should NOT be ignored, false otherwise"""
    if entry.name in ed:
        return False
    elif SD_PATTERN.match(entry.name) or NVME_PATTERN.match(entry.name):
        return True
    return False


def load_disks_from_dev(ed: tuple[str] | tuple) -> dict[str, str]:
    """Iterating over /dev is the safest route for getting a list of
    disks. One, non-obvious, reason for using /dev/ is that our HA
    systems will mount disks between the nodes across the heartbeat
    connection. These are used for iSCSI ALUA configurations. However,
    they are hidden and so don't surface in the /dev/ directory. If we
    were to use any other directory (/sys/block, /proc, etc) we run
    risk of enumerating those devices and breaking fenced."""
    disks = {}
    try:
        with scandir("/dev") as sdir:
            for disk in filter(lambda x: should_not_ignore(x, ed), sdir):
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
    return load_disks_from_dev(ed)
