"""Simulated storage redundancy: RAID-1 (mirroring) and RAID-5 (distributed XOR parity).

The fault-tolerant system applies the RAID-1 idea at database level (primary + synchronous replica); this module
models the underlying disk-array mechanics so that disk failure, degraded reads and rebuild can be demonstrated
and measured (see tests/test_hardware.py and `python -m hardware.raid_sim`).
"""
import hashlib
import os
import time


class DiskFailed(Exception):
    pass


class Disk:
    def __init__(self, name):
        self.name = name
        self.blocks = {}
        self.failed = False

    def write(self, idx, data):
        if self.failed:
            raise DiskFailed(self.name)
        self.blocks[idx] = data

    def read(self, idx):
        if self.failed:
            raise DiskFailed(self.name)
        return self.blocks[idx]


def xor(*bufs):
    out = bytearray(len(bufs[0]))
    for b in bufs:
        for i, v in enumerate(b):
            out[i] ^= v
    return bytes(out)


class Raid1:
    """N-way mirror: every write goes to all healthy disks; a read succeeds while one disk survives."""

    def __init__(self, n=2):
        self.disks = [Disk(f"mirror{i}") for i in range(n)]

    def write(self, idx, data):
        ok = 0
        for d in self.disks:
            try:
                d.write(idx, data)
                ok += 1
            except DiskFailed:
                pass
        if not ok:
            raise DiskFailed("all mirrors down")

    def read(self, idx):
        for d in self.disks:
            try:
                return d.read(idx)
            except (DiskFailed, KeyError):
                continue
        raise DiskFailed("all mirrors down")

    def rebuild(self, failed_disk):
        src = next(d for d in self.disks if not d.failed)
        failed_disk.failed = False
        failed_disk.blocks = dict(src.blocks)


class Raid5:
    """Block-level striping with rotating XOR parity over n >= 3 disks. Survives one disk failure."""

    def __init__(self, n=4, block=16):
        self.n, self.block = n, block
        self.disks = [Disk(f"disk{i}") for i in range(n)]
        self.stripes = 0

    def _layout(self, stripe):
        parity = (self.n - 1 - stripe) % self.n
        return parity, [i for i in range(self.n) if i != parity]

    def write_stripe(self, stripe, data_blocks):
        assert len(data_blocks) == self.n - 1 and all(len(b) == self.block for b in data_blocks)
        parity, data_disks = self._layout(stripe)
        for disk_i, blk in zip(data_disks, data_blocks):
            if not self.disks[disk_i].failed:
                self.disks[disk_i].write(stripe, blk)
        if not self.disks[parity].failed:
            self.disks[parity].write(stripe, xor(*data_blocks))
        self.stripes = max(self.stripes, stripe + 1)

    def read_stripe(self, stripe):
        parity, data_disks = self._layout(stripe)
        out, missing = [], None
        for pos, disk_i in enumerate(data_disks):
            try:
                out.append(self.disks[disk_i].read(stripe))
            except DiskFailed:
                if missing is not None:
                    raise DiskFailed("two disks lost: data unrecoverable")
                missing = pos
                out.append(None)
        if missing is not None:                                   # degraded read: reconstruct from parity
            survivors = [b for b in out if b is not None] + [self.disks[parity].read(stripe)]
            out[missing] = xor(*survivors)
        return out

    def rebuild(self, disk_i):
        d = self.disks[disk_i]
        d.failed, d.blocks = False, {}
        for s in range(self.stripes):
            parity, data_disks = self._layout(s)
            others = [self.disks[i].read(s) for i in range(self.n) if i != disk_i]
            d.blocks[s] = xor(*others)                            # lost block = XOR of all surviving blocks


def demo(stripes=2000, seed=3):
    rnd = __import__("random").Random(seed)
    r5 = Raid5(4, 16)
    original = []
    for s in range(stripes):
        blocks = [bytes(rnd.getrandbits(8) for _ in range(16)) for _ in range(3)]
        original.append(blocks)
        r5.write_stripe(s, blocks)
    digest = lambda rows: hashlib.sha256(b"".join(b"".join(r) for r in rows)).hexdigest()  # noqa: E731
    res = {"stripes": stripes}
    r5.disks[2].failed = True                                     # a disk dies
    t = time.perf_counter()
    degraded = [r5.read_stripe(s) for s in range(stripes)]
    res["degraded_read_intact"] = digest(degraded) == digest(original)
    res["degraded_read_s"] = round(time.perf_counter() - t, 4)
    t = time.perf_counter()
    r5.rebuild(2)
    res["rebuild_s"] = round(time.perf_counter() - t, 4)
    res["rebuild_intact"] = digest([r5.read_stripe(s) for s in range(stripes)]) == digest(original)
    r5.disks[0].failed = r5.disks[1].failed = True                # double failure exceeds RAID-5 tolerance
    try:
        r5.read_stripe(0)
        res["double_failure_survived"] = True
    except DiskFailed:
        res["double_failure_survived"] = False
    m = Raid1(2)
    m.write(0, b"transcript")
    m.disks[0].failed = True
    res["raid1_read_after_disk_loss"] = m.read(0) == b"transcript"
    m.rebuild(m.disks[0])
    res["raid1_rebuilt"] = m.disks[0].read(0) == b"transcript"
    return res


if __name__ == "__main__":
    print(demo())
