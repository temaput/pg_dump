#!/usr/bin/env python3
# vi:fileencoding=utf-8


"""
Python script to run pg_dump backuping on 1C server
Copyright © 2015 Artem Putilov

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from logging import getLogger, DEBUG, StreamHandler, WARNING  # noqa
log = getLogger("pg_dump")
log.addHandler(StreamHandler())
log.setLevel(WARNING)


import os
import glob
from datetime import date
from subprocess import Popen, PIPE, TimeoutExpired

BACKUP_DIR = "C:\\bases\\backup"
PGPASSWORD = "panurg"
DB_NAME = "art_tranzit"
TIMEOUT = 10 * 60 * 60
DAYS_IN_WEEK = 7
DAYS_IN_MONTH = 30


class BackupHistoryManager:
    """
    collects all archives in backup directory
    sorts them between monthly, weekly and daily groups according to pattern
    Setup pattern:
    dict(
    backup_dir = C:/...
    daily = 3,
    weekly = 3,
    monthly = 3
    )
    """
    daily = 3
    weekly = 3
    monthly = 1
    db_name = DB_NAME
    backup_dir = BACKUP_DIR

    def __init__(self, db_name=None, **kwargs):
        self.deleted_list = []
        if db_name is not None:
            self.db_name = db_name

    def process_bakups(self):
        today = date.today()
        if self.weekly > 0 or self.monthly > 0:
            # we need to save two daily to allow weekly or mon
            self.daily = max(self.daily, 2)
        fdelta = lambda fname: today - date.fromtimestamp(
            os.path.getmtime(fname))
        flist = sorted(
            [(fdelta(fname).days, fname) for fname in
             glob.glob(os.path.join(self.backup_dir, "*.backup"))],
            reverse=True)

        # save latest daily backups
        while(self.daily):
            flist.pop()
            self.daily -= 1
            if self.daily == 1:
                break  # leave one extra slot for earliest daily backup

        for days, fname in flist:

                if days >= DAYS_IN_MONTH:
                    if self.monthly > 0:  # we still have limit
                        month = days // DAYS_IN_MONTH
                        if month <= self.monthly:  # we are inside the limit
                            self.monthly = month - 1  # reduce limit
                            continue  # save the file
                elif days >= DAYS_IN_WEEK:
                    if self.weekly > 0:  # we still have limit
                        week = days // DAYS_IN_WEEK
                        if week <= self.weekly:  # we are inside the limit
                            self.weekly = week - 1  # reduce limit
                            continue  # save the file
                else:
                    # daily: we need to leave one earliest
                    if self.daily == 1:
                        self.daily -= 1
                        continue
                self.delete_file(fname)

    def delete_file(self, fname):
        if os.path.exists(fname):
            try:
                os.remove(fname)
                self.deleted_list.append(fname)
            except OSError:
                try:
                    import servicemanager
                    servicemanager.LogErrorMsg(
                        "BackupHistoryManager: could not delete file %s"
                        % fname)
                except ImportError:
                    pass

    def __call__(self):
        self.process_bakups()

    def print_stat(self):
        print("Removed %s files:" % len(self.deleted_list))
        for fname in sorted(self.deleted_list):
            print(fname)


class PgDumpCli:

    pg_dump_dir = "C:\\Program Files\\PostgreSQL\\9.2.4-1.1C\\bin\\"
    pg_dump_file = "pg_dump.exe"
    backup_format = "custom"
    username = "postgres"
    db_name = DB_NAME
    backup_dir = BACKUP_DIR

    def __init__(self, db_name=None, **kwargs):
        if db_name is not None:
            self.db_name = db_name

    @property
    def backup_file(self):
        return os.path.join(
            self.backup_dir,
            "%s-%s.backup" % (
                self.db_name,
                date.today())
        )

    @property
    def env(self):
        env = os.environ.copy()
        env["PGPASSWORD"] = PGPASSWORD
        return env

    def __call__(self):

        return (
            os.path.join(self.pg_dump_dir, self.pg_dump_file),
            '-F',
            self.backup_format,
            '-U',
            self.username,
            '-f',
            self.backup_file,
            self.db_name)


def main(db_name):
    import servicemanager
    pg_dump = PgDumpCli(db_name)
    servicemanager.LogInfoMsg(
        "Starting %s backup sequence" % pg_dump.db_name)

    try:
        p = Popen(pg_dump(), stderr=PIPE, stdout=PIPE, env=pg_dump.env)
        out, err = p.communicate(timeout=TIMEOUT)
        if p.returncode:
            # error code returned
            servicemanager.LogWarningMsg(
                "There was an error during %s backup.\n"
                "pg_dump response is:\n %s" % (
                    pg_dump.db_name, err.decode("cp866", errors='ignore')
                ))
        else:
            # successfull completion
            servicemanager.LogInfoMsg(
                "%s backup sequence complete" % pg_dump.db_name)
            history_manager = BackupHistoryManager(db_name)
            history_manager()
    except OSError:
        # bad command
        servicemanager.LogErrorMsg(
            "There was an error during %s backup.\n"
            "pg_dump.exe was not found at %s" % (
                pg_dump.db_name, pg_dump.pg_dump_dir)
        )
    except TimeoutExpired:
        p.kill()
        servicemanager.LogErrorMsg(
            "There was an error during %s backup.\n"
            "Timeout expired" % pg_dump.db_name
        )


if __name__ == "__main__":
    import sys
    db_name = DB_NAME
    if len(sys.argv) > 1:
        db_name = sys.argv[1]
    main(db_name)
