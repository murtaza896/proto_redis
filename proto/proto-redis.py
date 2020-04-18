from collections import defaultdict
import time


class ProtoRedis(object):
    def __init__(self):
        self.dict = {}
        self.expire = defaultdict(lambda: float('inf'))

    def __expired(self, key):
        return time.monotonic() > self.expire[key]

    def __exists(self, key):
        return key in self.dict

    def ping(self, message="PONG"):
        return message

    def set(self, key, val, exp_timer=("", 0), cond=""):
        # SET key val [EX secs| PX msecs] [NX set if key not exist| XX set if key exist] [KEEPTTL]
        timer = 0
        if exp_timer[0] == "EX" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]
        elif exp_timer[0] == "PX" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]/1000
        elif exp_timer[0] != "":
            return NotImplementedError

        if cond == "NX" and self.__exists(key):
            return -1
        elif cond == "XX" and not self.__exists(key):
            return -1
        elif cond != "":
            return NotImplementedError

        if key in self.expire:
            self.expire[key] = 0
        if timer:
            self.expire[key] = timer
        self.dict[key] = val
        return "OK"

    def get(self, key):
        if not self.__exists(key):
            return -1

        if self.__expired(key):
            del self.expire[key]
            del self.dict[key]
            return -1

        val = self.dict[key]
        return val

    def expire(self, key, seconds):
        if key not in self.expire or self.expire[key] == 0:
            return 0
        self.expire[key] = time.monotonic() + seconds
        return 1

    def ttl(self, key):
        if not self.__exists(key):
            return -2
        elif key not in self.expire:
            return -1
        elif self.__expired(key):
            del self.expire[key]
            del self.dict[key]
            return -2
        return int(self.expire[key] - time.monotonic())
