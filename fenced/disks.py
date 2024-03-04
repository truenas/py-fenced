import logging
from concurrent.futures import ThreadPoolExecutor, wait as fut_wait

from libsgio import SCSIErrorException, SCSI_OPCODES, SCSIDevice as SCSI
from nvme import NvmeDevice as NVME

SET_DISKS_CAP = 30
logger = logging.getLogger(__name__)


class Disks(dict):

    def __init__(self, fence):
        self.fence = fence
        self._set_disks = set()

    def add(self, disk):
        assert isinstance(disk, Disk)
        self[disk.name] = disk

    def remove(self, disk):
        self.pop(disk.name)

    def _get_set_disks(self):
        """
        There is no reason set keys on every disk, especially for systems with
        hundreds of them.
        For better performance let's cap the number of disks we set the new key
        per round to `SET_DISKS_CAP`.
        """
        if len(self) <= SET_DISKS_CAP:
            return self.values()

        newset = set(self.values()) - self._set_disks
        if newset:
            if len(newset) > SET_DISKS_CAP:
                newset = set(list(newset)[:SET_DISKS_CAP])
                self._set_disks.update(newset)
            else:
                newset.update(set(
                    list(
                        self.values()
                    )[:SET_DISKS_CAP - len(newset)]
                ))
                self._set_disks = newset
            return newset
        else:
            self._set_disks = set(list(self.values())[:SET_DISKS_CAP])
            return self._set_disks

    def _run_batch(self, method, args=None, disks=None, done_callback=None):
        """
        Helper method to run a batch of a Disk method
        """
        args = args or []
        disks = disks or self.values()
        executor = ThreadPoolExecutor(max_workers=SET_DISKS_CAP)
        try:
            fs = {
                executor.submit(getattr(disk, method), *args): disk
                for disk in disks
            }
            done_notdone = fut_wait(fs.keys(), timeout=30)
        finally:
            executor.shutdown(wait=False)
        failed = {fs[i] for i in done_notdone.not_done}
        if failed:
            logger.warning('method %r with args %r timed out for %d disk(s)', method, args, len(failed))
        for i in done_notdone.done:
            if done_callback:
                done_callback(i, fs, failed)
            else:
                try:
                    i.result()
                except Exception:
                    disk = fs[i]
                    logger.warning('method %r with args %r for disk %r failed.', method, args, disk, exc_info=True)
                    failed.add(disk)
        return failed

    def get_keys(self):
        keys = set()
        remote_keys = set()

        def callback(i, fs, failed):
            try:
                host_key, remote_key = i.result()
                remote_keys.update(remote_key)
                if host_key is None:
                    failed.add(fs[i])
                    return
                keys.add(host_key)
            except Exception:
                failed.add(fs[i])

        failed = self._run_batch('get_keys', done_callback=callback)
        return keys, remote_keys, failed

    def register_keys(self, newkey):
        return self._run_batch('register_key', [newkey], disks=self._get_set_disks())

    def reset_keys(self, newkey):
        return self._run_batch('reset_keys', [newkey])


class Disk(object):

    def __init__(self, fence, name, log_info=None):
        self.fence = fence
        self.name = name
        self.log_info = log_info
        self.curkey = None
        self.disk = NVME(f'/dev/{name}') if name.find('nvme') != -1 else SCSI(f'/dev/{name}')

    def __repr__(self):
        return f'<Disk: {self.name}>'

    def __str__(self):
        return self.name

    def get_keys(self):
        host_key = None
        remote_keys = set()
        for key in self.disk.read_keys()['keys']:
            # First 4 bytes are the host id
            if key >> 32 == self.fence.hostid:
                host_key = key
            else:
                remote_keys.add(key)
        return (host_key, remote_keys)

    def get_reservation(self):
        return self.disk.read_reservation()

    def register_key(self, newkey):
        newkey = self.fence.hostid << 32 | (newkey & 0xffffffff)
        self.disk.update_key(self.curkey, newkey)
        self.curkey = newkey

    def reset_keys(self, newkey):
        reservation = self.get_reservation()
        newkey = self.fence.hostid << 32 | (newkey & 0xffffffff)
        if reservation['reservation'] is not None:
            if reservation['reservation'] >> 32 != self.fence.hostid:
                # reservation isn't ours so register new key
                # and preempt the other reservation
                self.disk.register_ignore_key(newkey)
                try:
                    self.disk.preempt_key(reservation['reservation'], newkey)
                except SCSIErrorException as e:
                    if e.args and e.args[0] == SCSI_OPCODES.RESERVATION_CONFLICT:
                        # the logic by which we check if the reservation is "owned"
                        # by this host is custom logic that was written by us and is
                        # flawed. The spec for handling pr keys defines a command
                        # that can be used to determine if this host is the current
                        # reservation holder. Since we don't have this, we check
                        # for reservation conflict and try to "update" the key since
                        # getting a reservation conflict when trying to preempt the
                        # current key typically means this host is the current owner
                        self.disk.reserve_key(newkey)
            elif reservation['reservation'] >> 32 == self.fence.hostid:
                # reservation is owned by us so simply update
                # the existing reservation with the new key
                self.disk.update_key(reservation['reservation'], newkey)
        elif not self.disk.read_keys()['keys']:
            # check to see if there are even keys on disk
            self.disk.register_new_key(newkey)
            self.disk.reserve_key(newkey)
        else:
            self.disk.register_ignore_key(newkey)
            self.disk.reserve_key(newkey)
        self.curkey = newkey
