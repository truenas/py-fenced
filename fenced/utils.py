import json
import logging
import re
import subprocess

from pyudev import Context

from truenas_api_client import Client

logger = logging.getLogger(__name__)


def safe_retrieval(prop, keys, default, asint=False):
    for key in keys:
        if (value := prop.get(key)) is not None:
            if isinstance(value, bytes):
                value = value.strip().decode()
            else:
                value = value.strip()

            return value if not asint else int(value)

    return default


def get_disk_serial(dev):
    return safe_retrieval(
        dev.properties,
        ("ID_SCSI_SERIAL", "ID_SERIAL_SHORT", "ID_SERIAL"),
        "",
    )


def load_disks_middleware_no_zpools(c, ignore):
    # grab all detected disks on the system
    disks = {}
    try:
        disks = {
            k: v
            for k, v in c.call("device.get_disks", False, True).items()
            if not k.startswith(ignore[0]) and not ignore[1].match(k)
        }
    except Exception:
        logger.error("Unhandled exception", exc_info=True)

    return disks


def load_disks_middleware_use_zpools(c, ignore):
    disks = {}
    try:
        for i in c.call("pool.query"):
            for j in filter(
                lambda x: x["type"] == "DISK",
                c.call("pool.flatten_topology", i["topology"]),
            ):
                if (
                    j["disk"] is not None
                    and not j["disk"].startswith(ignore[0])
                    and not ignore[1].match(j["disk"])
                ):
                    disks[j["disk"]] = {"zpool": i["name"], "guid": i["guid"]}
    except Exception:
        logger.error("Unhandled exception", exc_info=True)

    return disks


def load_disks_pyudev(ignore):
    disks = {}
    try:
        for dev in Context().list_devices(subsystem="block", DEVTYPE="disk"):
            if dev.sys_name.startswith(ignore[0]) or ignore[1].match(dev.sys_name):
                continue

            disks[dev.sys_name] = get_disk_serial(dev)
    except Exception:
        logger.error("Unhandled exception", exc_info=True)

    return disks


def load_disks_last_resort(ignore):
    cmd = [
        "/usr/bin/lsblk",
        "-J",
        "-ndo",
        "NAME,SERIAL",
        "-I",
        "8,65,66,67,68,69,70,71,128,129,130,131,132,133,134,135,254,259",
    ]
    disks = {}
    try:
        disks = {
            i["name"]: i["serial"]
            for i in json.loads(
                subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode()
            )["blockdevices"]
            if not i["name"].startswith(ignore[0]) or ignore[1].match(i["name"])
        }
    except Exception:
        logger.error("Unhandled exception", exc_info=True)

    return disks


def disks_to_be_ignored(ed=None):
    prefixes = {"sr", "md", "dm-", "loop", "zd", "pmem"}
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
                prefixes.update(exclude)

    return (tuple(prefixes), re.compile(r"nvme[0-9]+c"))


def load_disks_impl(exclude_disks=None, use_zpools=False):
    """
    The name of the game for the various functions are ultimate fault tolerance.
    Since fenced is paramount for preventing zpool corruption, we do everything we
    can to prevent unexpected failures and always return disks.
    """
    disks_to_ignore = disks_to_be_ignored(ed=exclude_disks)
    disks = {}
    try:
        with Client() as c:
            if use_zpools:
                disks = load_disks_middleware_use_zpools(c, disks_to_ignore)

            if not disks:
                # load_disks_middleware_use_zpools can return nothing for reasons that aren't
                # fully understood...it's imperative that we reserve disks since it, ultimately,
                # prevents zpool corruption
                disks = load_disks_middleware_no_zpools(c, disks_to_ignore)
    except Exception:
        logger.error("Unhandled exception enumerating middleware client", exc_info=True)

    if not disks:
        # yikes....let's try again
        disks = load_disks_pyudev(disks_to_ignore)

    if not disks:
        # last resort...something is really broken
        disks = load_disks_last_resort(disks_to_ignore)

    return disks
