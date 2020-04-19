from collections import defaultdict
from sortedcontainers import SortedSet, SortedList

import time
import random

__all__ = ["ProtoRedis", "ZSet", "DBError"]


def decode(a, dtype):
    try:
        return dtype(a)
    except ValueError:
        raise DBError("Invalid conversion from one type to another")


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
            if 4 * (old_len - len(self.expired)) < old_len:
                break

    def log_dump(self, cmnd, *args):
        t = time.monotonic()
        with open('log.txt', 'a') as fw:
            fw.write(','.join([time, cmnd, *args]))

    def replay(self, after=0):
        with open('log.txt', 'r') as fw:
            for line in fw.readlines():
                curr_time = time.monotonic()
                split_line = line.split(",")
                tm, cmd, args = float(
                    split_line[0]), split_line[1], split_line[2:]
                if tm < after:
                    continue
                if cmd.lower() == b"set":
                    if b"ex" in args:
                        dur = int(args[args.index(b"ex") + 1])
                        if time.monotonic() > tm + dur:
                            continue
                    elif b"px" in args:
                        dur = int(args[args.index(b"px") + 1])
                        if time.monotonic() > tm + dur/1000:
                            continue
                    self.set(*args)
                elif cmd.lower() == b"expire":
                    dur = int(args)
                    if time.monotonic() > tm + dur:
                        continue
                    self.expire(dur)
                elif cmd.lower() == b"zadd":
                    self.zadd(*args)

    def ping(self, message="PONG"):
        return message

    def invalid(self, *args):
        raise DBError("ERR: Invalid Command")

    def set_(self, key, val, *args):
        # SET key val [EX secs| PX msecs] [NX set if key not exist| XX set if key exist] [KEEPTTL]
        i, px, ex, xx, nx = 0, None, None, False, False
        while i < len(args):
            if args[i].lower() == b"nx":
                nx = True
                i += 1
            elif args[i].lower() == b"xx":
                xx = True
                i += 1
            elif args[i].lower() == b"ex" and i + 1 < len(args):
                ex = decode(args[i + 1], int)
                if ex <= 0:
                    raise DBError("Invalid expire time")
                i += 2
            elif args[i].lower() == b"px" and i + 1 < len(args):
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
        self.log_dump('set')
        return True

    def get(self, key):
        if not self.__exists(key):
            return None

        if self.__have_expired(key):
            del self.expired[key]
            del self.cache[key]
            return None

        val = self.cache[key]
        return val

    def expire(self, key, seconds):
        if self.__have_expired(key) or not self.__exists(key):
            return 0
        self.expired[key] = time.monotonic() + seconds
        self.log_dump('expire')
        return 1

    def ttl(self, key):
        if not self.__exists(key):
            return -2
        elif key not in self.expired:
            return None
        elif self.__have_expired(key):
            del self.expired[key]
            del self.cache[key]
            return -2
        return int(self.expired[key] - time.monotonic())

    def zadd(self, key, *args):
        zset = self.get(key)
        if not zset:
            self.set_(key, ZSet())
            zset = self.get(key)

        i, nx, xx, ch, incr = 0, False, False, False, False
        while i < len(args):
            if args[i].lower() == b"nx":
                nx = True
                i += 1
            elif args[i].lower() == b"xx":
                xx = True
                i += 1
            elif args[i].lower() == b"ch":
                ch = True
                i += 1
            elif args[i].lower() == b"incr":
                incr = True
                i += 1
            else:
                break

        if nx and xx:
            raise DBError("Can't have both nx and xx in zadd")

        els = args[i:]
        if (incr and len(els) != 2) or (not els or len(els) % 2 != 0):
            raise DBError("Syntax Error")

        items = [(decode(els[j], float), els[j + 1])
                 for j in range(0, len(els), 2)]
        l_prev = len(zset)
        changed = 0

        if incr:
            score, mem = items
            score += zset.get(mem, 0)
            zset.add(mem, score)
            return score

        for score, mem in items:
            if (not nx or mem not in zset) and (not xx or mem in zset):
                changed += int(zset.add(mem, score))

        self.log_dump('zadd')
        if ch:
            return changed
        return len(zset) - l_prev

    def __zrange_generic(self, key, start, stop, reverse, *args):
        zset = self.get(key)
        if not zset:
            return None
        #print(args, type(args), args[0], type(args[0]))
        #print(key, start, stop, reverse, args)
        if len(args) > 1 or (args and args[0].lower() != b"withscores"):
            raise DBError("Syntax Error")
        start, stop = self._fix_range(start, stop, len(zset))
        if reverse:
            start, stop = len(zset) - stop, len(zset) - start
        items = zset.islice_score(start, stop, reverse)
        scored = bool(args)
        items = list(map(lambda x: (x[1], decode(x[0], float)), items))

        ans = []
        for item in items:
            ans.append(item[0])
            if scored:
                ans.append(str(item[1]))

        # print(ans)
        return ans

    def zrange(self, key, start, stop, *args):
        #print(key, start, stop, args)
        return self.__zrange_generic(key, int(start), int(stop), False, *args)

    def zrevrange(self, key, start, stop, *args):
        return self.__zrange_generic(key, int(start), int(stop), True, *args)

    def zrank(self, key, member):
        zset = self.get(key)
        if not zset:
            return None
        try:
            return zset.rank(member)
        except Exception:
            return None


class ZSet:
    def __init__(self):
        self.mem2score = {}
        self.scores = SortedList()

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

    def __str__(self):
        return self.message
