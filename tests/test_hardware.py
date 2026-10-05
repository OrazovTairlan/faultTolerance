import os
import sqlite3

import pytest

from hardware import backup, ecc_hamming as ecc, raid_sim as raid


def test_ecc_roundtrip_and_single_bit_correction():
    w = 0xDEADBEEFCAFEBABE
    cw = ecc.encode(w)
    assert ecc.decode(cw) == (w, "ok")
    for pos in range(72):                               # every single-bit flip is corrected
        assert ecc.decode(ecc.flip(cw, pos)) == (w, "corrected")


def test_ecc_detects_double_bit_errors():
    cw = ecc.encode(0x0123456789ABCDEF)
    for a, b in [(1, 2), (0, 70), (5, 64), (33, 34)]:
        assert ecc.decode(ecc.flip(cw, a, b))[1] == "uncorrectable"


def test_ecc_demo_statistics():
    s = ecc.demo(trials=500)
    assert s["single_wrong"] == 0 and s["double_missed"] == 0


def test_raid5_survives_one_disk_failure_and_rebuilds():
    r = raid.demo(stripes=200)
    assert r["degraded_read_intact"] and r["rebuild_intact"]
    assert r["double_failure_survived"] is False        # RAID-5 tolerates exactly one failure
    assert r["raid1_read_after_disk_loss"] and r["raid1_rebuilt"]


def test_raid1_total_loss():
    m = raid.Raid1(2)
    m.write(0, b"x")
    m.disks[0].failed = m.disks[1].failed = True
    with pytest.raises(raid.DiskFailed):
        m.read(0)


def test_backup_snapshot_restore(tmp_path):
    db = str(tmp_path / "a.db")
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE t(x)"); c.execute("INSERT INTO t VALUES(1)"); c.commit(); c.close()
    for _ in range(5):
        backup.snapshot(db, str(tmp_path / "bk"), keep=3)
    assert len(os.listdir(tmp_path / "bk")) == 3        # retention
    with open(db, "r+b") as f:
        f.write(b"garbage-garbage-garbage")
    backup.restore(str(tmp_path / "bk"), db)
    assert sqlite3.connect(db).execute("SELECT x FROM t").fetchone() == (1,)
