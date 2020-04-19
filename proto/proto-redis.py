from collections import defaultdict
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

    def set(self, key, val, exp_timer=("", 0), cond=""):
        # SET key val [EX secs| PX msecs] [NX set if key not exist| XX set if key exist] [KEEPTTL]
        timer = 0
        if exp_timer[0] == "ex" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]
        elif exp_timer[0] == "px" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]/1000
        elif exp_timer[0] != "":
            raise ValueError(
                "Expiry timer should have type EX or PX and time should be an integer")

        if cond == "nx" and self.__exists(key):
            return -1
        elif cond == "xx" and not self.__exists(key):
            return -1
        elif cond != "":
            raise ValueError("Existence condition should be either XX or EX")

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
        if key not in self.expired or self.expired[key] == 0:
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
