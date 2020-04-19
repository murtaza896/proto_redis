import asyncio
import operator
import hiredis
import sys

from collections import deque

meta = {"get": "_db.get"}


def serialize_to_wire(value):
    if isinstance(value, str):
        return ('+%s' % value).encode() + b'\r\n'
    elif isinstance(value, bool) and value:
        return b"+OK\r\n"
    elif isinstance(value, int):
        return (':%s' % value).encode() + b'\r\n'
    elif isinstance(value, bytes):
        return (b'$' + str(len(value)).encode() +
                b'\r\n' + value + b'\r\n')
    elif isinstance(value, Exception):
        return ('-%s' % str(value)).encode() + b'\r\n'
    elif value is None:
        return b'$-1\r\n'
    elif isinstance(value, list):
        base = b'*' + str(len(value)).encode() + b'\r\n'
        for item in value:
            base += serialize_to_wire(item)
        return base


class ProtoRedisProtocol(asyncio.Protocol):
    def __init__(self, db: ProtoRedis):
        self._db = db
        self.response = deque()
        self.parser = hiredis.Reader()

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport

    def data_received(self, data: bytes):
        self.parser.feed(data)

        while True:
            request = self.parser.gets()
            if not request:
                break
            resp = operator.methodcaller(
                meta.get(request[0], '_db.invalid'), *request[1:])(self)
            self.response.append(serialize_to_wire(resp))

        self.transport.writelines(self.response)
        self.response.clear()


def main() -> int:
    print("Hello, World!")

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()
    # Each client connection will create a new protocol instance
    coro = loop.create_server(ProtoRedisProtocol, "127.0.0.1", 7878)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("User Requested Shutdown")
    finally:
        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
