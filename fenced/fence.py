import enum
import logging
import sys
import time
import signal

from fenced.disks import Disk, SET_DISKS_CAP, run_batch
from fenced.exceptions import PanicExit
from fenced.utils import load_disks_from_dev

logger = logging.getLogger(__name__)
ID_FILE = "/etc/machine-id"


class ExitCode(enum.Enum):
    # middlewared imports this enum and maps return codes back to these
    # members, so the path, names and values must not change.
    REGISTER_ERROR = (1, "No disks available to register keys on")
    REMOTE_RUNNING = (2, "Fenced is running on the remote controller")
    RESERVE_ERROR = (3, "Too many disks failed to be reserved")
    EXCLUDE_DISKS_ERROR = (4, "Excluding all disks is not allowed")
    UNKNOWN = (5, "Unhandled exception")
    ALREADY_RUNNING = (6, "fenced is already running")
    NO_PANIC = (7, "fenced called with no panic flag")


class Fence(object):
    def __init__(self, interval, exclude_disks):
        self._interval = interval
        self._exclude_disks = exclude_disks
        self._disks = {}
        self._offset = 0
        self._reload = False
        self.hostid = None

    def get_hostid(self):
        try:
            with open(ID_FILE) as f:
                return int(f.read(8), 16)
        except Exception:
            logger.error("failed to generate unique id", exc_info=True)
            sys.exit(ExitCode.UNKNOWN.value[0])

    def load_disks(self):
        """
        Open every disk and read its keys. Returns the remote keys
        found and the names of the disks that failed to load.
        """
        logger.info("Loading disks")
        self._disks.clear()
        remote_keys, failed = set(), []
        for name in load_disks_from_dev(self._exclude_disks):
            # try 2 times to read the keys since there are SSDs
            # that have a firmware bug that will actually barf
            # on this request. However, nothing is wrong with
            # the disk. If you simply send the same request
            # again after this error, it will return the data
            # requested with no errors.
            # (i'm looking at you STEC ZeusRAM)
            for _ in range(2):
                try:
                    disk = Disk(name, self.hostid)
                    remote_keys |= disk.remote_keys()
                except Exception:
                    logger.warning("Failed to read keys for disk %r", name, exc_info=True)
                else:
                    self._disks[name] = disk
                    break
            else:
                failed.append(name)

        if failed:
            logger.error("Failed to load disks: %s", ",".join(failed))

        return remote_keys, failed

    def signal_handler(self, signum, frame):
        if signum == signal.SIGHUP:
            self._reload = True
        elif signum == signal.SIGUSR1:
            logger.info("SIGUSR1 received, disks: %s", ",".join(self._disks))

    def init(self, force):
        self.hostid = self.get_hostid()
        logger.info("Host ID: 0x%x.", self.hostid)

        remote_keys, failed = self.load_disks()
        total = len(self._disks) + len(failed)
        if not total:
            logger.error("No disks available, exiting.")
            sys.exit(ExitCode.REGISTER_ERROR.value[0])

        if not force:
            wait = 2 * self._interval + 1
            logger.info(
                "Waiting %d seconds to verify the reservation keys do not change.", wait
            )
            time.sleep(wait)
            results, _ = run_batch(Disk.remote_keys, self._disks.values())
            if not set().union(*results.values()).issubset(remote_keys):
                logger.error("Reservation keys have changed, exiting.")
                sys.exit(ExitCode.REMOTE_RUNNING.value[0])
            else:
                logger.info("Reservation keys unchanged.")

        newkey = int(time.time()) & 0xFFFFFFFF
        _, reset_failed = run_batch(Disk.reset_keys, self._disks.values(), newkey)
        # all disks on supported hardware do PR so failures on 10% or
        # more of them means something is badly wrong
        nfailed = len(failed) + len(reset_failed)
        if nfailed * 10 >= total:
            logger.error(
                "Failed to reset reservations on %d of %d disks, exiting.",
                nfailed,
                total,
            )
            sys.exit(ExitCode.RESERVE_ERROR.value[0])
        for disk in reset_failed:
            self._disks.pop(disk.name)

        logger.info("Persistent reservation set on %d disks.", len(self._disks))

        return newkey

    def _batch(self):
        """
        There is no reason to set keys on every disk, especially for systems
        with hundreds of them. Rotate through them `SET_DISKS_CAP` at a time.
        """
        disks = list(self._disks.values())
        if len(disks) <= SET_DISKS_CAP:
            return disks
        batch = [disks[(self._offset + i) % len(disks)] for i in range(SET_DISKS_CAP)]
        self._offset = (self._offset + SET_DISKS_CAP) % len(disks)
        return batch

    def loop(self, key):
        while True:
            if self._reload:
                logger.info("SIGHUP received, reloading.")
                key = self.init(True)
                self._reload = False

            key = 2 if key > 0xFFFFFFFF else key + 1
            logger.debug("Setting new key: 0x%x", key)
            _, failed = run_batch(Disk.register_key, self._batch(), key)
            for disk in failed:
                try:
                    resv = disk.reservation()
                except Exception:
                    logger.warning(
                        "Failed to get current reservation on %r",
                        disk.name,
                        exc_info=True,
                    )
                    self._disks.pop(disk.name)
                    continue

                if resv and resv >> 32 != self.hostid:
                    err = f"Reservation on {disk.name!r} preempted!"
                    logger.warning(err)
                    raise PanicExit(err)

                # getting here means we need to try and reset the reservations on the disk
                logger.warning("Trying to reset reservation for %r", disk.name)
                try:
                    disk.reset_keys(key)
                except Exception:
                    logger.warning(
                        "Failed to reset reservation on %r", disk.name, exc_info=True
                    )
                    self._disks.pop(disk.name)

            time.sleep(self._interval)
