import logging
import re
import os

logger = logging.getLogger(__name__)

SD_PATTERN = re.compile(r"^sd[a-z]+$")
NVME_PATTERN = re.compile(r"^nvme\d+n\d+$")


def disks_to_be_ignored(ed=None):
    if ed is not None:
        exclude = {}
        try:
            if isinstance(ed, list):
                # it's a list by default if fenced is called without exclude disk args
                exclude = {i for i in ed}
            elif isinstance(ed, str):
                # it's a comma separated list if fenced is called with exclude disk args
                exclude = {i for i in ed.split(",") if i}
        except Exception:
            logger.warning("Failed to format exclude disks params", exc_info=True)
        else:
            if exclude:
                return exclude


def should_not_ignore(entry, ed) -> bool:
    """Returns true if the device should NOT be ignored, false otherwise"""
    if ed is not None and entry.name in ed:
        return False
    elif SD_PATTERN.match(entry.name) or NVME_PATTERN.match(entry.name):
        return True
    return False


def load_disks_impl(exclude_disks=None, use_zpools=False):
    """
    The name of the game for the various functions are ultimate fault tolerance.
    Since fenced is paramount for preventing zpool corruption, we do everything we
    can to prevent unexpected failures and always return disks.
    """
    ed = disks_to_be_ignored(ed=exclude_disks)
    disks = {}
    try:
        with os.scandir("/dev") as sdir:
            for disk in filter(lambda x: should_not_ignore(x, ed), sdir):
                disks[disk.name] = disk.name
    except Exception:
        logger.error("Unhandled exception enumerating disks", exc_info=True)

    return disks
