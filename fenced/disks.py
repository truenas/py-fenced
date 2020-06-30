from concurrent.futures import ThreadPoolExecutor, wait as fut_wait

import libsgpersist
import logging

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
                newset.update(set(list(self.values())[:SET_DISKS_CAP - len(newset)]))
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
            logger.info('%s:%r timed out for %d disk(s)', method, args, len(failed))
        for i in done_notdone.done:
            if done_callback:
                done_callback(i, fs, failed)
            else:
                try:
                    i.result()
                except Exception as e:
                    disk = fs[i]
                    logger.debug('Failed to run %r %s:%r: %s', disk, method, args, e)
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

    def __init__(self, fence, name, pool=None):
        self.fence = fence
        self.name = name
        self.pool = pool
        self.curkey = None
        self.scsi = libsgpersist.SCSIDevice(f'/dev/{name}')
        self.nvme = None

    def __repr__(self):
        return f'<Disk: {self.name}>'

    def __str__(self):
        return self.name

    def get_keys(self):
        host_key = None
        remote_keys = set()

        keys = self.scsi.read_keys()['keys']

        for key in keys:
            # First 4 bytes are the host id
            if key >> 32 == self.fence.hostid:
                host_key = key
            else:
                remote_keys.add(key)

        return (host_key, remote_keys)

    def get_reservation(self):

        return self.scsi.read_reservation()

    def register_key(self, newkey):

        newkey = self.fence.hostid << 32 | (newkey & 0xffffffff)

        self.scsi.update_key(self.curkey, newkey)

        self.curkey = newkey

    def reset_keys(self, newkey):

        reservation = self.get_reservation()
        newkey = self.fence.hostid << 32 | (newkey & 0xffffffff)

        if reservation and reservation['reservation'] >> 32 != self.fence.hostid:
            # reservation isn't ours so register new key
            # and preempt the other reservation
            self.scsi.register_ignore_key(newkey)
            self.scsi.preempt_key(
                reservation['reservation'],
                newkey,
            )
        elif reservation and reservation['reservation'] >> 32 == self.fence.hostid:
            # reservation is owned by us so simply update
            # the existing reservation with the new key
            self.scsi.update_key(
                reservation['reservation'],
                newkey,
            )
        else:
            # check to see if there are even keys on disk
            keys = self.scsi.read_keys()['keys']

            if not keys:
                self.scsi.register_new_key(newkey)
                self.scsi.reserve_key(newkey)
            else:
                self.scsi.register_ignore_key(newkey)
                self.scsi.reserve_key(newkey)

        self.curkey = newkey
