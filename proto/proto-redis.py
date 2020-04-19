from collections import defaultdict
from sortedcontainers import SortedSet
import time


class ProtoRedis(object):
    def __init__(self):
        self.cache = {}
        self.expired = defaultdict(lambda: float('inf'))

    def __have_expired(self, key):
        return key in self.expired and time.monotonic() > self.expired[key]

    def __exists(self, key):
        return key in self.cache

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
                ex = int(args[i + 1]) if isinstance(args[i + 1], int) else 0
                if ex <= 0:
                    raise DBError("Invalid expire time")
                i += 2
            elif args[i] == "px" and i + 1 < len(args):
                px = int(args[i + 1]) if isinstance(args[i + 1], int) else 0
                if px <= 0:
                    raise DBError("Invalid expire time")
                i += 2
            else:
                raise DBError("Syntax Error")

        if (xx and nx) or (px is not None and ex is not None):
            raise DBError("Syntax Error")

        if nx and key:
            return None
        if xx and not key:
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

    def zadd(self, ss_key, cond="", changed="", incr=False, *args):
        pass

    def zrange(self, key, start, stop, scored=False):
        pass

    def zrank(self, key, member):
        pass


class ZSet:
    def __init__(self):
        self.mem2score = {}
        self.scores = SortedSet()

    def add(self, val, score):
        s_prev = self.mem2score.get(val, None)
        if s_prev:
            if s_prev == score:
                return False
            self.scores.remove((s_prev, val))

        pass


class Error(Exception):
    pass


class DBError(Error):
    def __init__(self, message):
        self.message = message
