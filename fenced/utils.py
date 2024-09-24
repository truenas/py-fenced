import json
import logging
import re
import subprocess

from truenas_api_client import Client

logger = logging.getLogger(__name__)


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


def load_disks_lsblk(ignore):
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
    first_attempt_disks = {}
    try:
        with Client() as c:
            if use_zpools:
                first_attempt_disks = load_disks_middleware_use_zpools(
                    c, disks_to_ignore
                )

            if not first_attempt_disks:
                # load_disks_middleware_use_zpools can return nothing for reasons that aren't
                # fully understood...it's imperative that we reserve disks since it, ultimately,
                # prevents zpool corruption
                first_attempt_disks = load_disks_middleware_no_zpools(
                    c, disks_to_ignore
                )
    except Exception:
        logger.error("Unhandled exception enumerating middleware client", exc_info=True)

    # we've discovered that the ES24F, when fully populated with SAS SSD
    # SEAGATE XS7680SE70084 0002, has a race condition where-by these disks
    # haven't been cached in udevd's database during a failover event.
    # This is particularly painful for the following scenario:
    #   Have head-unit populated with disks, have ES24F fully populated. Failover
    #   is triggered and fenced enumerates ONLY the disks in the head-unit. Since
    #   we enumerated _SOME_ disks, we assume everything is fine and continue on
    #   by design. However, when we go to import the zpool on the other controller
    #   the import fails because the drives in the shelf were not enumerated thereby
    #   preventing persistent reservations from being updated which ultimately ends
    #   up with the zpool(s) failing to be imported. This is a production outage
    #   scenario that requires administrative intervention which is painful.
    #
    # To combat this scenario, we will enumerate disks using middleware and then
    # enumerate disks using a different method. We will take the larger of the
    # enumerated disk objects. This was tested in the field on a customer system.
    second_attempt_disks = load_disks_lsblk(disks_to_ignore)
    if not first_attempt_disks:
        if not second_attempt_disks:
            # yikes....this is bad first and second attempt failed...
            # just try 1 last time to return something
            logger.warning(
                "Two attempts enumerating disks failed. Trying one last time."
            )
            return load_disks_lsblk(disks_to_be_ignored)
        else:
            logger.warning(
                "First attempt enumerating disks failed but second attempt succeeded."
            )
            return second_attempt_disks

    len_first, len_second = len(first_attempt_disks), len(second_attempt_disks)
    if len_first > len_second:
        logger.warning(
            "First attempt enumerated more disks (%r) than second attempt (%r).",
            len_first,
            len_second,
        )
        return first_attempt_disks
    elif len_second > len_first:
        if use_zpools:
            # use_zpools will place reservations on disks associated to zpool(s)
            # which means the 2nd attempt could produce more drives since the
            # zpool(s) might not being using all disks on the system. This is
            # fine.
            return first_attempt_disks
        else:
            logger.warning(
                "Second attempt enumerated more disks (%r) than first attempt (%r)",
                len_second,
                len_first,
            )
            return second_attempt_disks

    # first and second attempt are equal
    return first_attempt_disks
