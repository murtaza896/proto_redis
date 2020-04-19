# Proto Redis

## A redis prototype

This is a protoype of a Redis database attached to a TCP server written in Python. The TCP server supports Redis RESP Protocol and hence clients like `redis-cli` can be used to communicate with it.

## Example

```python
    from proto.server import main
    main('127.0.0.1','6972') # Runs server on local
```

now connect with `redis-cli`. Any client will do. Even async clients.

## Commands

The following commands have been implemented as of yet:

- `PING`
- `GET key val`
- `SET key val [EX sec|PX milli] [NX | XX]`
- `EXPIRE key secs`
- `TTL key`
- `ZADD key [CH] [NX|EX] [INCR] [score member]+`
- `ZRANGE key start stop [WITHSCORES]`
- `ZREVRANGE key start stop [WITHSCORES]`
- `ZRANK key member`

## Key Expiration

The key-value stre is maintained using a dictionary(hashtable). Now repeatative checks to see when a key has expired is very expensive. Hence, I used the strategy of lazy deletion. In lazy deletion only during the time of reading, if the key has expired we delete it. To check a key has expired or not we keep yet another hashtable with key and time of expiry as key value pair. This makes the `EXPIRY` consume `O(1)` time. However, redundant space may still be left since we fail to guarantee that read is bound to happen in the near future. Hence, to mitigate this issue, I've used an non-deterministic algorithm.

```python
    In every 10 times a sec
    do
        sample 20 keys from expired hashtable
        delete all these keys if they have expired
    while (oldlength - newlength)/oldlength >= .25  
```

This probabilistic algorithm does it's job fairly well. Hence it is incorporated even in the original redis. Now, in my implementation this is roughly 0.1 secs. I keep a chronometer which measures the difference better the last two requests. If it's not 0.1 sec we extend the chronometer until it hits more than 0.1 sec. Then I do the clear.

## Persistence

In Redis ther are 3 types of persistence:

- Snapshot : In snapshot persistence, a snapshot of the memory is taken and saved in a particular localtion whenever asked. In case of system failure, we can reload our last backed up data and snapshot histories and the cost of losing recent data.
- Logged : In logged persistence, all the write operations are dumped in a log file. In case of failure, we can playback the log file and reconstruct the recent state at the cost of losing historical data.  
- Both : In this approach, both the methods are used to reconstruct the recent and the previous data set and then a new snapshot and logs are backedup.

Redis allows all the three combinations, I just only implemented Logged approach for redis is popular for being a cache first and thena permanent datbase. Hence recent seemed to be a good choice given the time constraints. I implemented a special command called `replay` which can be called from the clients to order the db to reconstruct itself.

## Concurrency

The TCP serevr I made uses a asynchronous input-output system to talk to multiple clients. In fact, with minor modifications this architecture can be extended to support Pub-Sub Operations, and even Blocking ones. For purging the keys after each 0.1 sec, I could have used multiple processes. Same would apply for writing data to the log files or the backup files. However, I am not a huge fan of processes and threads. This is because deadlocks are hard to spot, and with enough locks even the most non-deterministic systems becomes deterministic. Hence, I prefer using asynchronous input - output.

## Why Python?

I chose Python because it produces short-code, has support of data-structures out of the box, has an asyncio Protocol builder. Interpreted laangues are not as fast as compiled languages, however with good algorithms they are fairly scalable.

## Limitations

- Supports less commands.
- Doesn't support Pub Sub.
- Doesn't support replication.
- Doesn't support blocking operations.
- Doesn't do snapshot storage.
- Slower than Redis which is written in C.

## Future Work

- Implement Pub Sub.
- Implement blocking operations.
- Implement snapshot storage and replication.
- Use as much asyncio as possible (especially for cron based expiry check, and for log and snapshot updating).
