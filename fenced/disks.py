import logging
from concurrent.futures import ThreadPoolExecutor, wait

from fenced.scsi_generic import inquiry

from libsgio import SCSIErrorException, SCSIDevice as SCSI
from nvme import NvmeDevice as NVME

SET_DISKS_CAP = 30
logger = logging.getLogger(__name__)


def run_batch(fn, disks, *args, timeout=30):
    """
    Call `fn(disk, *args)` for each disk concurrently.
    Returns ({disk: result}, {disks that failed or timed out}).

    The executor is not used as a context manager because its exit waits
    for every thread, so one hung disk would block past `timeout`.
    """
    executor = ThreadPoolExecutor(max_workers=SET_DISKS_CAP)
    futs = {executor.submit(fn, disk, *args): disk for disk in disks}
    done, not_done = wait(futs, timeout=timeout)
    executor.shutdown(wait=False, cancel_futures=True)

    results, failed = {}, {futs[f] for f in not_done}
    if failed:
        logger.warning("%s timed out for %d disk(s)", fn.__name__, len(failed))
    for f in done:
        try:
            results[futs[f]] = f.result()
        except Exception:
            logger.warning("%s failed for %r", fn.__name__, futs[f], exc_info=True)
            failed.add(futs[f])
    return results, failed


class Disk:
    def __init__(self, name, hostid):
        self.name = name
        self.hostid = hostid
        self.curkey = None
        path = f"/dev/{name}"
        if name.startswith("nvme"):
            # enumerated as a proper nvme device
            self.dev = NVME(path)
        elif inquiry(path)["vendor"].lower() == "nvme":
            # tri-mode HBA on h-series platform enumerates
            # nvme devices as SCSI devices. Must check the
            # T10 vendor string from the device itself to
            # see if it's a NVMe device.
            self.dev = NVME(path)
        else:
            self.dev = SCSI(path)

    def __repr__(self):
        return f"<Disk: {self.name}>"

    def remote_keys(self):
        # First 4 bytes are the host id
        return {k for k in self.dev.read_keys()["keys"] if k >> 32 != self.hostid}

    def reservation(self):
        return self.dev.read_reservation()["reservation"]

    def register_key(self, newkey):
        newkey = self.hostid << 32 | (newkey & 0xFFFFFFFF)
        self.dev.update_key(self.curkey, newkey)
        self.curkey = newkey

    def reset_keys(self, newkey):
        resv = self.reservation()
        newkey = self.hostid << 32 | (newkey & 0xFFFFFFFF)
        if resv is not None:
            if resv >> 32 != self.hostid:
                # reservation isn't ours so register new key
                # and preempt the other reservation
                self.dev.register_ignore_key(newkey)
                try:
                    self.dev.preempt_key(resv, newkey)
                except SCSIErrorException:
                    # ignored on purpose, fenced has always swallowed preempt errors
                    pass
            else:
                # reservation is owned by us so simply update
                # the existing reservation with the new key
                self.dev.update_key(resv, newkey)
        elif not self.dev.read_keys()["keys"]:
            # check to see if there are even keys on disk
            self.dev.register_new_key(newkey)
            self.dev.reserve_key(newkey)
        else:
            self.dev.register_ignore_key(newkey)
            self.dev.reserve_key(newkey)
        self.curkey = newkey
