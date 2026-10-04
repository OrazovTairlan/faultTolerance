"""ECC memory demonstration: extended Hamming SECDED (72,64) code.

Server DRAM uses exactly this kind of code: 64 data bits + 8 check bits per word.
  * any single-bit error  -> detected and corrected,
  * any double-bit error  -> detected (uncorrectable, machine-check / page retired).
The implementation is a software model that lets us inject bit flips ("cosmic ray" faults) and observe the effect.
"""
import random

DATA_BITS = 64
PARITY_BITS = 7                    # 2^7 = 128 >= 64 + 7 + 1
N = DATA_BITS + PARITY_BITS        # 71 Hamming positions (1..71), plus 1 overall parity bit = 72


def _is_pow2(x):
    return x & (x - 1) == 0


_DATA_POS = [p for p in range(1, N + 1) if not _is_pow2(p)]      # positions that carry data bits


def encode(word: int):
    """64-bit integer -> list of 72 bits: index 0 = overall parity, index p (1..71) = Hamming positions."""
    code = [0] * (N + 1)
    for i, pos in enumerate(_DATA_POS):
        code[pos] = (word >> i) & 1
    for k in range(PARITY_BITS):
        p = 1 << k
        code[p] = sum(code[pos] for pos in range(1, N + 1) if pos & p and pos != p) % 2
    code[0] = sum(code[1:]) % 2
    return code


def decode(code):
    """-> (word, status) with status in {'ok', 'corrected', 'uncorrectable'}."""
    c = list(code)
    syndrome = 0
    for k in range(PARITY_BITS):
        p = 1 << k
        if sum(c[pos] for pos in range(1, N + 1) if pos & p) % 2:
            syndrome |= p
    overall = sum(c) % 2
    if syndrome == 0 and overall == 0:
        status = "ok"
    elif overall == 1:                      # odd number of flips -> assume single error and fix it
        if syndrome == 0:
            c[0] ^= 1                       # the overall parity bit itself flipped
        elif syndrome <= N:
            c[syndrome] ^= 1
        else:
            return None, "uncorrectable"
        status = "corrected"
    else:                                   # even number of flips with non-zero syndrome -> double error
        return None, "uncorrectable"
    word = 0
    for i, pos in enumerate(_DATA_POS):
        word |= c[pos] << i
    return word, status


def flip(code, *positions):
    c = list(code)
    for p in positions:
        c[p] ^= 1
    return c


def demo(trials=2000, seed=1):
    rng = random.Random(seed)
    stats = {"no_error": 0, "single_corrected": 0, "single_wrong": 0, "double_detected": 0, "double_missed": 0}
    for _ in range(trials):
        w = rng.getrandbits(64)
        cw = encode(w)
        if decode(cw) != (w, "ok"):
            stats["single_wrong"] += 1
        else:
            stats["no_error"] += 1
        d, s = decode(flip(cw, rng.randrange(72)))
        stats["single_corrected" if (d == w and s == "corrected") else "single_wrong"] += 1
        a, b = rng.sample(range(72), 2)
        d, s = decode(flip(cw, a, b))
        stats["double_detected" if s == "uncorrectable" else "double_missed"] += 1
    stats["trials"] = trials
    return stats


if __name__ == "__main__":
    print(demo())
