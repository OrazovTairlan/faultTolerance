"""Backup infrastructure: consistent point-in-time snapshots (SQLite online-backup API) and restore."""
import glob
import os
import sqlite3
import time


def snapshot(src, dst_dir, keep=3):
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, f"snapshot-{time.strftime('%Y%m%d-%H%M%S')}-{int(time.time() * 1000) % 1000:03d}.db")
    s = sqlite3.connect(src, timeout=2.0)
    d = sqlite3.connect(dst)
    try:
        s.backup(d)
    finally:
        d.close()
        s.close()
    for old in sorted(glob.glob(os.path.join(dst_dir, "snapshot-*.db")))[:-keep]:
        try:
            os.remove(old)
        except OSError:
            pass
    return dst


def latest(dst_dir):
    files = sorted(glob.glob(os.path.join(dst_dir, "snapshot-*.db")))
    return files[-1] if files else None


def verify(path):
    c = sqlite3.connect(path)
    try:
        return c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        c.close()


def restore(dst_dir, target):
    """Restore the newest healthy snapshot over `target`."""
    for snap in reversed(sorted(glob.glob(os.path.join(dst_dir, "snapshot-*.db")))):
        if verify(snap):
            s = sqlite3.connect(snap)
            tmp = target + ".restore"
            d = sqlite3.connect(tmp)
            try:
                s.backup(d)
            finally:
                d.close()
                s.close()
            os.replace(tmp, target)
            return snap
    raise RuntimeError("no healthy snapshot available")
