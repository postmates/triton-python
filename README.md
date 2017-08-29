# Triton Project

Python Utility For Building Data Pipelines for  AWS Kinesis & Google Pubsub.

See also - [Triton](https://github.com/postmates/go-triton)

### Configuration

#### Kinesis

Normal AWS credential environment variables or IAM roles for boto apply.

Clients will need to define a yaml file containing definitions for the streams
they will want to use. That yaml file will look like:

```yaml
my_stream:
  name: my_stream_v2
  partition_key: value
  region: us-west-1
```

Clients generating records will reference the `my_stream` stream which will
automatically know to use the real underlying stream of `my_stream_v2` in the
`us-west-1` region. Records put into this stream are assumed to have a key
named `value` which is use for partitioning.

#### Pubsub

Triton can also be configured to use [Google Pubsub](https://cloud.google.com/pubsub/docs/).

The following config entry enables interaction with the Pubsub topic `my_pubsub_stream`:

```yaml
my_pubsub_stream:
  provider: gcp
  project: my_project
  topic: my_topic
  private_key_file: /path/to/service/account/private.key
```

Note - It is assumed that the given service account private key provides the proper permissions to the topic - read access for consumption and/or write access for publication.

Given Pubsub's simplified interface, clients are not able to control which storage nodes in the data plane a given message is published to.  As is the case with Kinesis streams.

#### Multiplexing

Streams can also be configured to represent multiple downstream providers.  For example, the stream alias `my_composite_stream` can be configured to publish events to both Kinesis and Pubsub as follows:

```yaml
my_composite_stream:
  -
    provider: aws
    name: my_kinesis_stream
    partition_key: value
    region: us-west-1

  -
    provider: gcp
    project: my_project
    topic: my_topic
    private_key_file: /path/to/service/account/private.key
```

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

__CAVEAT UTILITOR__: Triton currently only supports data types directly convertible
into [msgpack formated data](https://github.com/msgpack/msgpack/blob/master/spec.md).
Unsupported types will raise a `TypeError`.

### Non-Blocking Producers and `tritond`

Using the producer syntax above, `s.put(value='hi mom', ts=time.time())`, will block until
the operation to put the data is complete, which can take on the order of 100 ms.
This guarantees that the write has succeeded before continuing the flow of control.

To allow for writes that do not block, triton comes with `tritond`;
a daemon that will spool events to local memory and write those messages to the configured backend asynchronously.
Writes via this pathway block for approximately 0.1 ms.  `tritond` flushes events every 100ms.
It is important to note that using this non-blocking pathway eliminates the guarantee

By default, `tritond` will listen on `127.0.0.1:3515` or it will
respect the environment variables `TRITON_ZMQ_HOST` and `TRITON_ZMQ_PORT`.
The `tritond` uses the same `triton.yaml` files to configure triton streams;
and will _log errors and skip_ any data if the stream is not configured
or the config file is not found.

`tritond` can be run by simply calling it from the command line. For testing
and/or debugging, it can be run in verbose mode and with its output directed to stdout or a file e.g.

    tritond -v --skip-kinesis  # writes verbose logs and writes events to stdout

    tritond -cc --skip-kinesis --output_file test_output.txt

Once `tritond` is running, usage follows the basic write pattern:

    import triton

    c = triton.load_config("/etc/triton.yaml")

    s = triton.get_nonblocking_stream('my_stream', c)
    s.put(value='hi mom', ts=time.time())

Also, as mentioned above, Triton currently only supports data types directly convertible
into [msgpack formated data](https://github.com/msgpack/msgpack/blob/master/spec.md).
For data put into a `NonblockingStream` object, unsupported types will log an error and continue.

### Consumers

#### Kinesis

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

##### Checkpointing

Triton supports checkpointing to a DB so that processing can start where
previous processing left off. It requires a postgresDB available.
To specify the DB location, set the ENV variable `TRITON_DB` to the DSN
of the postgres DB, e.g.

    export TRITON_DB="dbname=db_name port=5432 host=www.dbhosting.com user=user_name password=password"

Attempting to checkpoint without this DB being configured will raise a
`TritonCheckpointError` exception.

The DB also needs to have a specific table created; calling the following will initialized the table (this call is safe to repeat; it is a no-op if the table already exists):

    triton.checkpoint.init_db()

Triton checkpointing also requires a unique client name, since the basic
assumption is that the checkpoint DB will be shared. The client name is specified
by the ENV variable `TRITON_CLIENT_NAME`.
Attempting to checkpoint without this ENV variable will also raise a
`TritonCheckpointError` exception.


Once configured, checkpointing can be used simply by calling the `checkpoint`
method on a stream iterator.

For example:

    s = triton.get_stream('my_stream', c)
    i = s.build_iterator_from_checkpoint()

    for ctr in range(1):
        rec = i.next()
        print rec.data

    i.checkpoint()

The next time this code is run, it will pick up from where the last run left off.


##### Consuming Archives

Triton data is typically archived to S3. Using the triton command, you can view that data:

    $ triton cat --bucket=triton-data --stream=my_stream --start-date=20150715 --end-date=20150715

Or using the API, something like:

    import triton

    c = triton.load_config("/etc/triton.yaml")
    b = triton.open_bucket("triton-data", "us-west-1")
    s = triton.stream_from_s3_store(b, c['my_stream'], start_dt, end_dt)

    for rec in s:
        ... do something ...

#### Pubsub

The following example sets up a Pubsub consumer which pulls messages from the horizon of the configured topic until Google closes connection due to inactivity:

```python
import triton

config = triton.load_config("/etc/triton.yaml")
stream = triton.get_stream("my_gcp_stream", config)

for message in s:
   print message
```

Note - Messages are acknowledged as new messages are returned from the iterator.  Furthermore, a subscription named as `triton-uuid.uuid4.hex` is created on behalf of the user.

The above example can also be expressed as follows in order to handle cases where publishers may not publish events for a sufficiently long period of time:

```
import triton

config = triton.load_config("/etc/triton.yaml")
stream = triton.get_stream("my_gcp_stream", config)
iterator = triton.get_iterator_from_latest()

for message in iterator:
   print message
```

Pubsub consumers can also resume from a named subscription:

```python
import triton

subscription_id = 'my-subscription'
config = triton.load_config("/etc/triton.yaml")
stream = triton.get_stream("my_gcp_stream", config)

for message in s.build_iterator_from_checkpoint(subscription_id):
   print message
```

Note - The user must create the subscription prior to consumption otherwise an exception will be raised.

## Development

You should be able to configure your development environment using make:

    ~/python-triton $ make dev

You will likely need to install system libraries as well:

    ~/python-triton $ sudo apt-get install libsnappy-dev libzmq-dev

The tests should all work:

    ~/python-triton $ make test
    .
    PASSED.  1 test / 1 case: 1 passed, 0 failed.  (Total test time 0.00s)

If you need to debug your application with ipython:

    ~/python-triton $ make shell
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
