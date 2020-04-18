from collections import defaultdict
table = {}
expire = defaultdict(lambda: float('inf'))


class ProtoRedis(object):
    def __init__(self):
        self.dict = table

    def ping(self, message="PONG"):
        return message

    def set(self, *args):
        pass

    def get(self, key):
        pass

    def expire(self, key, seconds):
        pass

    def ttl(self, key):
        pass
