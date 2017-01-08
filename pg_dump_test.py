import unittest
from pg_dump import BackupHistoryManager

import os
from datetime import date, timedelta
import time
import random
from itertools import permutations
import glob
import shutil


def create_tmp_dir():
    dname = os.path.join(
        os.path.expanduser("~/tmp"),
        "BackupHistoryManagerTest-%s" % date.today()
    )

    while(os.path.exists(dname)):
        dname += "%s" % random.randint(10, 99)
    os.makedirs(dname)
    return dname


def fill_backups(dname, db_name):
    t = date.today()
    for days, weeks, months in permutations((0, 0, 1, 2, 3, 1), 3):
        tdelta = timedelta(days + weeks * 7 + months * 30)
        fdate = t - tdelta
        fname = os.path.join(
            dname,
            "%s-%s.backup" % (db_name, fdate)
        )
        if not os.path.exists(fname):
            open(fname, "w").close()
            ftime = time.mktime(fdate.timetuple())
            os.utime(fname, times=(ftime, ftime))
    flist = glob.glob(os.path.join(dname, "*.backup"))
    print("Created %s mockup files:" % len(flist))
    for fname in sorted(flist):
        print(fname)


class TestBackupHistoryManager(unittest.TestCase):

    def setUp(self):
        self.hm = BackupHistoryManager()
        self.tmpdir = create_tmp_dir()
        self.hm.backup_dir = self.tmpdir
        fill_backups(self.tmpdir, "test_db_name")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_one_daily(self):
        self.hm.daily = 1
        self.hm.monthly = 0
        self.hm.weekly = 0
        self.hm()

        files = glob.glob(os.path.join(self.tmpdir, "*.backup"))
        print("Left %s mockup files:" % len(files))
        for fname in sorted(files):
            print(fname)
        self.assertGreaterEqual(len(files), 1)
        self.assertLessEqual(len(files), 2)

    def test_one_each(self):
        self.hm.daily = 1
        self.hm.monthly = 1
        self.hm.weekly = 1
        self.hm()

        files = glob.glob(os.path.join(self.tmpdir, "*.backup"))
        print("Left %s mockup files:" % len(files))
        for fname in sorted(files):
            print(fname)
        self.assertGreaterEqual(len(files), 3)
        self.assertLessEqual(len(files), 4)

    def test_two_each(self):
        self.hm.daily = 2
        self.hm.monthly = 2
        self.hm.weekly = 2
        self.hm()

        files = glob.glob(os.path.join(self.tmpdir, "*.backup"))
        print("Left %s mockup files:" % len(files))
        for fname in sorted(files):
            print(fname)
        self.assertGreaterEqual(len(files), 6)
        self.assertLessEqual(len(files), 7)

    def test_three_each(self):
        self.hm.daily = 3
        self.hm.monthly = 3
        self.hm.weekly = 3
        self.hm()

        files = glob.glob(os.path.join(self.tmpdir, "*.backup"))
        print("Left %s mockup files:" % len(files))
        for fname in sorted(files):
            print(fname)
        self.assertGreaterEqual(len(files), 9)
        self.assertLessEqual(len(files), 10)

if __name__ == "__main__":
    unittest.main()
