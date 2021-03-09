#!/usr/bin/env python3

import argparse
import logging
import os
import signal
import struct
import sys
import time
import contextlib

from fenced.exceptions import PanicExit, ExcludeDisksError
from fenced.fence import Fence, ExitCode
from fenced.logging import setup_logging
from middlewared.client import Client

logger = logging.getLogger(__name__)
ALERT_FILE = '/data/sentinels/.fence-alert'
PID_FILE = '/tmp/.fenced-pid'


def is_running():
    """
    Check if there is a currently running fenced process.
    Multiple fenced processes running on the same system will
    clobber one another and will, ultimately, cause a panic.
    Ticket #48031
    Note: we need to make sure that if there is a running
    fenced process, it's not ourself :)
    """
    running = False
    with contextlib.suppress(Exception):
        with Client() as c:
            data = c.call('failover.fenced.run_info')
            if data['running'] and data['pid']:
                if data['pid'] != os.getpid():
                    running = True

    return running


def update_pid_file():
    """
    We call this method after we have fork()'ed
    """
    with contextlib.suppress(Exception):
        with open(PID_FILE, 'w+') as f:
            f.write(str(os.getpid()))


def panic(reason):
    """
    An unclean reboot is going to occur.
    Try to create this file and write epoch time to it. After we panic,
    middlewared will check for this file, read the epoch time in the file
    and will send an appropriate email and then remove it.
    Ticket #39114
    """
    try:
        with open(ALERT_FILE, 'wb') as f:
            epoch = int(time.time())
            b = struct.pack('@i', epoch)
            f.write(b)
            f.flush()
            os.fsync(f.fileno())  # Be extra sure
    except Exception as e:
        logger.debug('Failed to write alert file: %r', e)

    logger.error('FATAL: %s', reason)
    logger.error('FATAL: issuing an immediate panic.')

    # enable the "magic" sysrq-triggers
    # https://www.kernel.org/doc/html/latest/admin-guide/sysrq.html
    with open('/proc/sys/kernel/sysrq', 'w') as f:
        f.write('1')

    # now violently reboot
    with open('/proc/sysrq-trigger', 'w') as f:
        f.write('b')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Do not check existing disk reservations',
    )
    parser.add_argument(
        '--foreground', '-F',
        action='store_true',
        help='Run in foreground mode',
    )
    parser.add_argument(
        '--no-panic', '-np',
        action='store_true',
        help='Do not panic in case of a fatal error',
    )
    parser.add_argument(
        '--interval', '-i',
        default=5,
        type=int,
        help='Time in seconds between each SCSI reservation set/check',
    )
    parser.add_argument(
        '--exclude-disks', '-ed',
        default=[],
        help='List of disks to be excluded from SCSI reservations.'
             ' (THIS CAN CAUSE PROBLEMS IF YOU DONT KNOW WHAT YOURE DOING)',
    )
    parser.add_argument(
        '--use-zpools', '-uz',
        action='store_true',
        help='Reserve the disks in use by the zpools detected on this system',
    )
    args = parser.parse_args()

    setup_logging(args.foreground)

    if is_running():
        logger.error('fenced already running.')
        sys.exit(ExitCode.ALREADY_RUNNING.value)

    fence = Fence(args.interval, args.exclude_disks, args.use_zpools)
    newkey = fence.init(args.force)

    if not args.foreground:
        logger.info('Entering in daemon mode.')
        if os.fork() != 0:
            sys.exit(0)
        os.setsid()
        if os.fork() != 0:
            sys.exit(0)
        os.closerange(0, 3)
    else:
        logger.info('Running in foreground mode.')

    update_pid_file()

    signal.signal(signal.SIGHUP, fence.signal_handler)
    signal.signal(signal.SIGUSR1, fence.signal_handler)

    try:
        fence.loop(newkey)
    except PanicExit as e:
        if args.no_panic:
            logger.info('Fatal error: %s', e)
            sys.exit(ExitCode.UNKNOWN.value)
        else:
            logger.error('PANIC: %s', e)
            panic(e)
    except ExcludeDisksError as e:
        logger.error('FATAL: %s', e)
        sys.exit(ExitCode.EXCLUDE_DISKS_ERROR.value)
    except Exception:
        logger.error('Unhandled exception', exc_info=True)
        sys.exit(ExitCode.UNKNOWN.value)


if __name__ == '__main__':
    main()
