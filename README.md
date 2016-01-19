# Triton Project

Python Utility code for building a Data Pipeline with AWS Kinesis.

See [Triton](https://github.com/postmates/go-triton)

Kinesis (http://aws.amazon.com/kinesis/) lets you define streams of records.
You put records in one end, and the other end can consumer them. The stream
maintains the records for 24 hours. These streams come in multiple shards
(defined by the adminstrator).

The tooling provided here builds on top of the boto library to make real-world
work with these streams and record easier. This is preference to using the
Amazon provided KCL (Kinesis Client Library) which is Java-based, or the python
bindings built on top of KCL, because it isn't very pythonic.

### Configuration

Normal AWS credential environment variables or IAM roles for boto apply.

Clients will need to define a yaml file containing definitions for the streams
they will want to use. That yaml file will look like:

    my_stream:
      name: my_stream_v2
      partition_key: value
      region: us-west-1

Clients generating records will reference the `my_stream` stream which will
automatically know to use the real underlying stream of `my_stream_v2` in the
`us-west-1` region. Records put into this stream are assumed to have a key
named `value` which is use for partitioning.


### Demo

Triton comes with a command line script `triton` which can be used to demo some simple functionality.

    $ echo 'hi' | triton put -s test_stream

And then to consume:

    $ triton get -s test_stream
    <Record shardId-000000000001 49551479315998225220804774498660962603757541393499684882>
    {'msg': 'hi\n', 'ts': 1433969276.172019}

(Note the order is actually important here, this consumer is set to 'latest',
so if your producer produces first, you might miss it.)
You can set the config by using the environment variable TRITON_CONFIG, the default is /etc/triton.yaml

### Producers

Adding records to the stream is easy:

    import triton

    c = triton.load_config("/etc/triton.yaml")

    s = triton.get_stream('my_stream', c)
    s.put(value='hi mom', ts=time.time())


For more advanced uses, you can record the shard and sequence number returned
by the put operation.

    shard, seq_num = s.put(...)

You could in theory communicate these values to some other process if you want
to ensure they have received this record.

__CAVEAT UTILITOR__: Triton currently only supports data types directly converatible
into [msgpack formated data](https://github.com/msgpack/msgpack/blob/master/spec.md).
Unsupported types will raise a `TypeError`.

### Non-Blocking Producers and `tritond`

Using the producer syntax above, `s.put(value='hi mom', ts=time.time())`, will block until
the operation to put the data into Kinesis is complete, which can take on the order of 100 ms.
This guarantees that the write has succeeded before continuing the flow of control.

To allow for writes that do not block, triton comes with `tritond`;
a daemon that will spool Kinesis messages to local memory and write those messages to Kinesis asynchronously.
Writes via this pathway block for approximately 0.1 ms.
The `tritond` spools messages to memory and writes all recieved messages to Kinesis
every 100 ms.
It is important to note that using this non-blocking pathway eliminates the guarantee
that data will be written to Kinesis.

An instance of `tritond` needs be running to collect Kinesis writes.
It is recomended to run an instance on each host that will be producing Kinesis writes.
By default, `tritond` will listen on `127.0.0.1:3515` or it will
respect the environment variables `TRITON_ZMQ_HOST` and `TRITON_ZMQ_PORT`.
The `tritond` uses the same `triton.yaml` files to configure triton streams;
and will _log errors and skip_ any data if the stream is not configured
or the config file is not found.

Once `tritond` is running, usage follows the basic write pattern:

    import triton

    c = triton.load_config("/etc/triton.yaml")

    s = triton.get_nonblocking_stream('my_stream', c)
    s.put(value='hi mom', ts=time.time())

Since the actual Kinesis write happens asynchronously, the shard and sequence number
are not returned from this operation. 
Also, as mentioned above, Triton currently only supports data types directly converatible
into [msgpack formated data](https://github.com/msgpack/msgpack/blob/master/spec.md).
For data put into a `NonblockingStream` object, unsupported types will log an error and continue.

### Consumers

Writing consumers is more complicated as you must deal with sharding. Even in
the lightest of workloads you'll likely want to have multiple shards. Triton makes this simple:

    import triton

    c = triton.load_config("/etc/triton.yaml")

    s = triton.get_stream('my_stream', c)
    i = s.build_iterator_from_latest()

    for rec in i:
        print rec.data

This will consume only new records from the stream. Since the stream in theory
never ends, you can in your own process management tell it when to stop:

    for rec in i:

        do_stuff(rec)

        if has_reason():
            i.stop()

This will cause the iterator to stop fetching new data, but will flush out data
that's already been fetched.

Kinesis supports other types of iterators. For example, if you want to see all the data in the stream:

    i = s.build_iterator_for_all()

or if you know a specific shard and offset:

    i = s.build_iterator_from_seqnum(shard_num, seq_num)

For building distributed consumers, you'll want to divide up the work by shards.
So if you have 4 shards, the first worker would:

    i = s.build_iterator_from_latest([0, 1])

and the second worker would do:

    i = s.build_iterator_from_latest([2, 3])

Note that these are 'share numbers', not shard ids. These are indexes into the
actual shard list.

### Consuming Archives

Triton data is typically archived to S3. Using the triton command, you can view that data:

    $ triton cat --bucket=triton-data --stream=my_stream --start-date=20150715 --end-date=20150715

Or using the API, something like:

    import triton

    c = triton.load_config("/etc/triton.yaml")
    b = triton.open_bucket("triton-data", "us-west-1")
    s = triton.stream_from_s3_store(b, c['my_stream'], start_dt, end_dt)

    for rec in s:
        ... do something ...


## Development

You should be able to configuration your development environment using make:

    ~/Projects/Project $ make dev

The tests should all work:

    ~/Projects/Project $ make test
    .
    PASSED.  1 test / 1 case: 1 passed, 0 failed.  (Total test time 0.00s)

If you need to debug your application with ipython:

    ~/Projects/Project $ make shell
    Python 2.7.3 (default, Apr 27 2012, 21:31:10)
    Type "copyright", "credits" or "license" for more information.

    IPython 0.12.1 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: from project.models import Project

    In [2]:

## TODO

  * It would probably be helpful to have some common code for building a worker
    process that just handles records.
