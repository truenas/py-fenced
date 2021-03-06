import enum
import logging
import sys
import time
import signal

from fenced.disks import Disk, Disks
from fenced.exceptions import PanicExit, ExcludeDisksError
from middlewared.client import Client


logger = logging.getLogger(__name__)

ID_FILE = '/etc/hostid'


class ExitCode(enum.IntEnum):
    REGISTER_ERROR = 1
    REMOTE_RUNNING = 2
    RESERVE_ERROR = 3
    EXCLUDE_DISKS_ERROR = 4
    UNKNOWN = 5
    ALREADY_RUNNING = 6


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
            with open(ID_FILE, 'rb') as f:
                return int(f.read(4).hex(), 16)
        except Exception as e:
            logger.error('failed to generate unique id with error: %s', e)
            sys.exit(ExitCode.UNKNOWN.value)

    def load_disks(self):

        logger.debug('Loading disks')
        self._disks.clear()
        unsupported = []
        remote_keys = set()

        try:
            with Client() as c:
                if not self._use_zpools:
                    # grab all detected disks on the system
                    disks = c.call('device.get_info', 'DISK')
                else:
                    # detect zpool(s) and add the disks in use by said zpool(s)
                    disks = {}
                    for i in c.call('pool.query'):
                        for j in c.call('pool.flatten_topology', i['topology']):
                            if j['type'] == 'DISK' and j['disk'] is not None:
                                disks[j['disk']] = {
                                    'zpool': i['name'],
                                    'guid': i['guid'],
                                }
        except Exception:
            logger.error('failed to generate disk info', exc_info=True)
            sys.exit(ExitCode.UNKNOWN.value)

        if disks and not set(disks) - set(self._exclude_disks):
            # excluding all disks is not allowed
            raise ExcludeDisksError('Excluding all disks is not allowed')

        for k, v in disks.items():

            # You can pass an "-ed" argument to fenced
            # to exclude disks from getting SCSI reservations.
            # fenced is called with this option by default
            # to exclude the OS boot drive(s).
            if k in self._exclude_disks:
                continue

            # used when SIGUSR1 is sent to us we will log info
            # to be used for troubleshooting purposes
            if self._use_zpools:
                log_info = v
            else:
                log_info = {
                    'serial': v['serial'],
                    'type': v['type'],
                }

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
                    disk = Disk(self, k, log_info=log_info)
                    remote_keys.update(disk.get_keys()[1])
                except Exception:
                    logger.debug(f'Retrying to read keys for disk: {k}')
                    if i < tries - 1:
                        continue
                    else:
                        unsupported.append(k)

            self._disks.add(disk)

        if unsupported:
            logger.debug(
                'Disks without support for SCSI-3 PR: %s.',
                ' '.join(unsupported)
            )

        return remote_keys

    def signal_handler(self, signum, frame):
        if signum == signal.SIGHUP:
            self._reload = True
        elif signum == signal.SIGUSR1:
            logger.debug('SIGUSR1 received, logging information')
            self.log_info()

    def init(self, force):
        self.hostid = self.get_hostid()
        logger.info('Host ID: 0x%x.', self.hostid)

        remote_keys = self.load_disks()
        if not self._disks:
            logger.error('No disks available, exiting.')
            sys.exit(ExitCode.REGISTER_ERROR.value)

        if not force:
            wait_interval = 2 * self._interval + 1
            logger.info(
                'Waiting %d seconds to verify remote keys.',
                wait_interval
            )
            time.sleep(wait_interval)
            new_remote_keys = self._disks.get_keys()[1]
            if not new_remote_keys.issubset(remote_keys):
                logger.error('Remote keys have changed, exiting.')
                sys.exit(ExitCode.REMOTE_RUNNING.value)
            else:
                logger.info('Remote keys unchanged.')

        newkey = int(time.time()) & 0xffffffff
        failed_disks = self._disks.reset_keys(newkey)
        if failed_disks:
            rate = int((len(failed_disks) / len(self._disks)) * 100)
            if rate > 10:
                logger.error(
                    '%d%% of the disks failed to reset SCSI-3 PRs, exiting.',
                    rate
                )
                sys.exit(ExitCode.RESERVE_ERROR.value)
            for disk in failed_disks:
                self._disks.remove(disk)

        logger.info('SCSI reservation set on %d disks.', len(self._disks))

        return newkey

    def log_info(self):
        info = {}
        for v in self._disks.values():
            info.update({v.name: v.log_info})
        logger.debug(info)

    def loop(self, key):

        while True:

            if self._reload:
                logger.warning('SIGHUP received, reloading.')
                key = self.init(True)
                self._reload = False

            if key > 0xffffffff:
                key = 2
            else:
                key += 1

            logger.debug('Setting new key: 0x%x', key)
            failed_disks = self._disks.register_keys(key)
            if failed_disks:
                for disk in list(failed_disks):
                    try:
                        reservation = disk.get_reservation()
                        if reservation:
                            reshostid = reservation['reservation'] >> 32
                            if self.hostid != reshostid:
                                raise PanicExit(
                                    'Reservation for disk (%s) was preempted.',
                                    disk.name
                                )

                        logger.info(
                            'Trying to reset reservation for %s',
                            disk.name
                        )
                        disk.reset_keys(key)
                        failed_disks.remove(disk)
                    except PanicExit:
                        raise
                    except Exception:
                        pass
                if failed_disks:
                    logger.info(
                        'Failed to set reservations on: %s so removing',
                        ', '.join(d.name for d in failed_disks),
                    )
                for d in failed_disks:
                    self._disks.remove(d)

            time.sleep(self._interval)
