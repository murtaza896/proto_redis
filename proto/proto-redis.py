from collections import defaultdict
import time

table = {}
expire = defaultdict(lambda: float('inf'))


class ProtoRedis(object):
    def __init__(self):
        self.dict = table

    def __expired(self, key):
        return time.monotonic() > expire[key]

    def __exists(self, key):
        return key in self.dict

    def ping(self, message="PONG"):
        return message

    def set(self, key, val, exp_timer=("", 0), cond="", keepttl=False):
        # SET key val [EX secs| PX msecs] [NX set if key not exist| XX set if key exist] [KEEPTTL]
        timer = 0
        if exp_timer[0] == "EX" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]
        elif exp_timer[0] == "PX" and type(exp_timer[1], int):
            timer = time.monotonic() + exp_timer[1]/1000
        elif exp_timer[0] != "":
            return NotImplementedError

        if cond == "NX" and key in self.dict:
            return -1
        elif cond == "XX" and key not in self.dict:
            return -1
        elif cond != "":
            return NotImplementedError

    def get(self, key):
        if not self.__exists:
            return -1

        if self.__expired(key):
            del expire[key]
            del self.dict[key]
            return -1

        val = self.dict[key]
        return len(val), val

    def expire(self, key, seconds):
        pass

    def ttl(self, key):
        pass
