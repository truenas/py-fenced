import enum
import logging
import sys
import time
import signal

from fenced.disks import Disk, Disks
from fenced.exceptions import PanicExit, ExcludeDisksError
from fenced.utils import load_disks_impl

logger = logging.getLogger(__name__)
ID_FILE = '/etc/machine-id'


class ExitCode(enum.Enum):
    REGISTER_ERROR = (1, 'No disks available to register keys on')
    REMOTE_RUNNING = (2, 'Fenced is running on the remote controller')
    RESERVE_ERROR = (3, 'Too many disks failed to be reserved')
    EXCLUDE_DISKS_ERROR = (4, 'Excluding all disks is not allowed')
    UNKNOWN = (5, 'Unhandled exception')
    ALREADY_RUNNING = (6, 'fenced is already running')
    NO_PANIC = (7, 'fenced called with no panic flag')


class Fence(object):

    def __init__(self, interval, exclude_disks, use_zpools):
        self._interval = interval
        self._exclude_disks = exclude_disks
        self._use_zpools = use_zpools
        self._disks = Disks(self)
        self._reload = False
        self.hostid = None

    def get_hostid(self):
        try:
            with open(ID_FILE) as f:
                return int(f.read(8), 16)
        except Exception:
            logger.error('failed to generate unique id', exc_info=True)
            sys.exit(ExitCode.UNKNOWN.value[0])

    def load_disks(self):
        logger.info('Clearing disks (if any)')
        self._disks.clear()

        logger.info('Loading disks')
        try:
            disks = load_disks_impl(self._exclude_disks, self._use_zpools)
        except Exception:
            logger.error('unhandled exception enumerating disk info', exc_info=True)
            sys.exit(ExitCode.UNKNOWN.value[0])
        else:
            if disks and not set(disks) - set(self._exclude_disks):
                raise ExcludeDisksError('Excluding all disks is not allowed')

        unsupported = []
        remote_keys = set()
        for disk_name, disk_info in disks.items():
            # try 2 times to read the keys since there are SSDs
            # that have a firmware bug that will actually barf
            # on this request. However, nothing is wrong with
            # the disk. If you simply send the same request
            # again after this error, it will return the data
            # requested with no errors.
            # (i'm looking at you STEC ZeusRAM)
            tries = 2
            for i in range(tries):
                try:
                    disk = Disk(self, disk_name, log_info=disk_info)
                    remote_keys.update(disk.get_keys()[1])
                except Exception:
                    logger.warning('Retrying to read keys for disk %r', disk_name)
                    if i < tries - 1:
                        continue
                    else:
                        unsupported.append(disk_name)
            self._disks.add(disk)

        if unsupported:
            logger.warning('Disks without support for SCSI-3 PR: %s', ','.join(unsupported))

        return remote_keys

    def signal_handler(self, signum, frame):
        if signum == signal.SIGHUP:
            self._reload = True
        elif signum == signal.SIGUSR1:
            logger.info('SIGUSR1 received, logging information')
            self.log_info()

    def init(self, force):
        self.hostid = self.get_hostid()
        logger.info('Host ID: 0x%x.', self.hostid)

        remote_keys = self.load_disks()
        if not self._disks:
            logger.error('No disks available, exiting.')
            sys.exit(ExitCode.REGISTER_ERROR.value[0])

        if not force:
            wait = 2 * self._interval + 1
            logger.info('Waiting %d seconds to verify the reservation keys do not change.', wait)
            time.sleep(wait)
            new_remote_keys = self._disks.get_keys()[1]
            if not new_remote_keys.issubset(remote_keys):
                logger.error('Reservation keys have changed, exiting.')
                sys.exit(ExitCode.REMOTE_RUNNING.value[0])
            else:
                logger.info('Reservation keys unchanged.')

        newkey = int(time.time()) & 0xffffffff
        failed_disks = self._disks.reset_keys(newkey)
        if failed_disks:
            rate = int((len(failed_disks) / len(self._disks)) * 100)
            if rate > 10:
                logger.error('Failed to reset reservations on %d%% of the disks, exiting.', rate)
                sys.exit(ExitCode.RESERVE_ERROR.value[0])
            for disk in failed_disks:
                self._disks.remove(disk)

        logger.info('Persistent reservation set on %d disks.', len(self._disks))

        return newkey

    def log_info(self):
        logger.info(', '.join([f'{v.name}: {v.log_info}' for v in self._disks.values()]))

    def loop(self, key):
        while True:
            if self._reload:
                logger.info('SIGHUP received, reloading.')
                key = self.init(True)
                self._reload = False

            key = 2 if key > 0xffffffff else key + 1
            logger.debug('Setting new key: 0x%x', key)
            for failed_disk in list(self._disks.register_keys(key)):
                try:
                    resv = failed_disk.get_reservation()
                except Exception:
                    logger.warning('Failed to get current reservation on %r', failed_disk.name, exc_info=True)
                    self._disks.remove(failed_disk)
                    continue
                else:
                    if all((resv, resv['reservation'])) and self.hostid != (resv['reservation'] >> 32):
                        err = f'Reservation on {failed_disk.name!r} preempted!'
                        logger.warning(err)
                        raise PanicExit(err)

                # getting here means we need to try and reset the reservations on the disk
                logger.warning('Trying to reset reservation for %r', failed_disk.name)
                try:
                    failed_disk.reset_keys(key)
                except Exception:
                    logger.warning('Failed to reset reservation on %r', failed_disk.name, exc_info=True)
                    self._disks.remove(failed_disk)
                    pass

            time.sleep(self._interval)
