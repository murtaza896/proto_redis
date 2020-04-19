from collections import defaultdict
from sortedcontainers import SortedSet

import time
import random


def decode(a, dtype):
    try:
        ans = dtype(a)
    except ValueError:
        raise DBError("Invalid conversion from one type to another")
    return ans


class ProtoRedis(object):
    def __init__(self):
        self.cache = {}
        self.expired = defaultdict(lambda: float('inf'))

    def __have_expired(self, key):
        return key in self.expired and time.monotonic() > self.expired[key]

    def __exists(self, key):
        return key in self.cache

    @staticmethod
    def _fix_range(lo, hi, length):
        if lo < 0:
            lo = max(0, lo + length)
        if hi < 0:
            hi += length
        if lo > hi or lo >= length:
            return -1, -1
        hi = min(hi, length - 1)
        return lo, hi + 1

    def purger(self):
        while True:
            old_len = len(self.expired)
            smpl_sz = min(20, old_len)
            sample = random.sample(expired.keys(), smpl_sz)
            for k in sample:
                if self.expired[k] == 0 or time.monotonic() - self.expired[k] < 0:
                    del self.cache[k]
                    del self.expired[k]
            if (1 - len(self.expired)/old_len) < .25:
                break

    def ping(self, message="PONG"):
        return message

    def set(self, key, val, *args):
        # SET key val [EX secs| PX msecs] [NX set if key not exist| XX set if key exist] [KEEPTTL]
        i, px, ex, xx, nx = 0, None, None, False, False
        while i < len(args):
            if args[i] == "nx":
                nx = True
                i += 1
            elif args[i] == "xx":
                xx = True
                i += 1
            elif args[i] == "ex" and i + 1 < len(args):
                ex = decode(args[i + 1], int)
                if ex <= 0:
                    raise DBError("Invalid expire time")
                i += 2
            elif args[i] == "px" and i + 1 < len(args):
                px = decode(args[i + 1], int)
                if px <= 0:
                    raise DBError("Invalid expire time")
                i += 2
            else:
                raise DBError("Syntax Error")

        if (xx and nx) or (px is not None and ex is not None):
            raise DBError("Syntax Error")

        if (nx and key) or (xx and not key):
            return None

        timer = 0
        if ex is not None:
            timer = time.monotonic() + ex
        if px is not None:
            timer = time.monotonic() + px / 1000.0

        if key in self.expired:
            self.expired[key] = 0
        if timer:
            self.expired[key] = timer
        self.cache[key] = val
        return "OK"

    def get(self, key):
        if not self.__exists(key):
            return -1

        if self.__have_expired(key):
            del self.expired[key]
            del self.cache[key]
            return -1

        val = self.cache[key]
        return val

    def expire(self, key, seconds):
        if self.__have_expired(key) or not self.__exists(key):
            return 0
        self.expired[key] = time.monotonic() + seconds
        return 1

    def ttl(self, key):
        if not self.__exists(key):
            return -2
        elif key not in self.expired:
            return -1
        elif self.__have_expired(key):
            del self.expired[key]
            del self.cache[key]
            return -2
        return int(self.expired[key] - time.monotonic())

    def zadd(self, key, *args):
        zset = self.get(key)
        if zset == -1:
            self.set(key, ZSet())
            zset = self.get(key)

        i, nx, xx, ch = 0, False, False, False
        while i < len(args):
            if args[i] == "nx":
                nx = True
                i += 1
            elif args[i] == "xx":
                xx = True
                i += 1
            elif args[i] == "ch":
                ch = True
                i += 1
            else:
                break

        if nx and xx:
            raise DBError("Can't have both nx and xx in zadd")

        els = args[i:]
        if not els or len(els) % 2 != 0:
            raise DBError("Syntax Error")

        items = [(decode(els[j], float), els[j + 1])
                 for j in range(0, len(els), 2)]
        l_prev = len(zset)
        changed = 0

        for score, mem in items:
            if (not nx or mem not in zset) and (not xx or mem in zset):
                changed += int(zset.add(mem, score))

        if ch:
            return changed
        return len(zset) - l_prev

    def __zrange_generic(self, key, start, stop, reverse, *args):
        zset = self.get(key)
        if zset == -1:
            return []
        if len(args) > 1 or (args and args[0] != "withscores"):
            raise DBError("Syntax Error")
        start, stop = self._fix_range(start, stop, len(zset))
        if reverse:
            start, stop = len(zset) - stop, len(zset) - start
        items = zset.islice_score(start, stop, reverse)
        scored = bool(args)
        items = list(map(lambda y: y if scored else y[0], map(
            lambda x: (x[1], decode(x[0], float)), items)))
        return items

    def zrange(self, key, start, stop, *args):
        return self.__zrange_generic(key, start, stop, False, args)

    def zrevrange(self, key, start, stop, *args):
        return self.__zrange_generic(key, start, stop, True, args)

    def zrank(self, key, member):
        zset = self.get(key)
        if zset == -1:
            return None
        try:
            return zset.rank(member)
        except Exception:
            return None


class ZSet:
    def __init__(self):
        self.mem2score = {}
        self.scores = SortedSet()

    def __contains__(self, val):
        return val in self.mem2score

    def __setitem__(self, val, score):
        self.add(val, score)

    def __getitem__(self, key):
        return self.mem2score[key]

    def __len__(self):
        return len(self.mem2score)

    def __iter__(self):
        def f():
            for score, val in self.scores:
                yield val
        return f()

    def get(self, key, default=None):
        return self.mem2score.get(key, default)

    def add(self, val, score):
        s_prev = self.mem2score.get(val, None)
        if s_prev:
            if s_prev == score:
                return False
            self.scores.remove((s_prev, val))
        self.mem2score[val] = score
        self.scores.add((score, val))
        return True

    def discard(self, key):
        try:
            score = self.mem2score.pop(key)
        except KeyError:
            return
        self.scores.remove((score, key))

    def items(self):
        return self.mem2score.items()

    def rank(self, member):
        return self.scores.index((self.mem2score[member], member))

    def islice_score(self, start, stop, reverse=False):
        return self.scores.islice(start, stop, reverse)


class Error(Exception):
    pass


class DBError(Error):
    def __init__(self, message):
        self.message = message
