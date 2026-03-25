# snctl-cpp

The CLI tool to manage clusters on StreamNative Cloud.

## How to install

### (Recommended) Use pre-built binaries

Currently the release only supports
- macOS 14 or later: arm64 architecture only
- Alpine Linux 3.21 or later: amd64 or amd64 architecture

Take v0.1.0 for example:

```bash
export VERSION=0.1.0
curl -O -L https://github.com/BewareMyPower/snctl-cpp/releases/download/v$VERSION/snctl-cpp-macos-14-arm64.zip
unzip -q snctl-cpp-macos-14-arm64.zip
```

Now, you can run `snctl-cpp -h` to see the help message.

### Build from source

You must have a C++ compiler that supports C++17.

```bash
git submodule update --init --recursive
cmake -B build
cmake --build build
cp build/snctl-cpp .
```

## Configuration

By default, `snctl-cpp` will look for configurations from the following paths in order, which can be configured by the `--config` option:
1. The `sncloud.ini` in the current active directory.
2. `~/.snctl-cpp/sncloud.ini`.

If no file exists in the paths above, `snctl-cpp` will create a new file in the current active directory with the default configs:

```ini
[kafka]
bootstrap.servers = localhost:9092
token =
fetch.message.max.bytes = 52428800
max.partition.fetch.bytes = 52428800
fetch.max.bytes = 52428800
isolation.level = read_uncommitted
```

To modify the config, you can edit the config file directly, whose path can be retrieved by the `./snctl-cpp --get-config` command.

The alternative way is to use the `configs` subcommand so that you can modify the config without opening the file. Here is an example that `snctl-cpp` is put into the `/private/tmp/` directory and there is no config file in the default paths:

```bash
$ ./snctl-cpp configs update --kafka-url localhost:9093
No config file found. Creating /private/tmp/sncloud.ini with the default configs
Updated bootstrap.servers to localhost:9093
Updated config file /private/tmp/sncloud.ini
$ ./snctl-cpp configs update --kafka-token my-token
Updated token
Updated config file /private/tmp/sncloud.ini
$ ./snctl-cpp configs update --kafka-fetch-message-max-bytes 52428800
Updated fetch.message.max.bytes to 52428800
Updated config file /private/tmp/sncloud.ini
$ ./snctl-cpp configs update --kafka-max-partition-fetch-bytes 52428800
Updated max.partition.fetch.bytes to 52428800
Updated config file /private/tmp/sncloud.ini
$ ./snctl-cpp configs update --kafka-fetch-max-bytes 52428800
Updated fetch.max.bytes to 52428800
Updated config file /private/tmp/sncloud.ini
$ ./snctl-cpp configs update --kafka-isolation-level read_committed
Updated isolation.level to read_committed
Updated config file /private/tmp/sncloud.ini
```

## Commands

**NOTE**: The commands below assumes `snctl-cpp` is in the `PATH`.

### Topics

#### Create a topic

```bash
$ snctl-cpp topics create tp0
Created topic "tp0" with 1 partition
$ snctl-cpp topics create tp1 -p 5
Created topic "tp1" with 5 partitions
```

#### Delete a topic

```bash
$ snctl-cpp topics delete tp
Failed to delete topic "tp": Broker: Unknown topic or partition
$ snctl-cpp topics delete tp0
Deleted topic "tp0"
```

#### Describe a topic

Query the owner brokers for all partitions:

```bash
$ snctl-cpp topics describe <topic>
Partition[0] leader: {"id": 816909419, url: "pb0-<xxx>:9093"}"
Partition[1] leader: {"id": 101337027, url: "pb4-<xxx>:9093"}"
...
Partition[15] leader: {"id": 644587507, url: "pb2-<xxx>:9093"}"
```

Query the owner brokers for all partitions in a specific zone (`use1-az1` in this case):

```bash
$ snctl-cpp --client-id zone_id=use1-az1 topics describe <topic>
Partition[0] leader: {"id": 1868363245, url: "pb5-<xxx>:9093"}
Partition[1] leader: {"id": 1868363245, url: "pb5-<xxx>:9093"}
...
Partition[15] leader: {"id": 644587507, url: "pb2-<xxx>:9093"}
```

As you can see, when a client specifies `use1-az1` as its zone, only brokers in the same zone (`pb2` and `pb5`) will serve the requests from that client.

#### List topics

List all topics and print the number of partitions for each topic:

```bash
$ snctl-cpp topics list
topic count: 2
[0] "my-topic-2" with 1 partition
[1] "my-topic-1" with 10 partitions
```

## Consumer Groups

### List all consumer groups

```bash
$ snctl-cpp groups list
There are 1 group
[0] sub Stable
```

### Describe a specific consumer group

```bash
$ snctl-cpp groups describe sub
Group ID: sub
Assignor: range
State: Stable
Type: 2
There are 2 members:
| index | client id | consumer id | host | assignments |
| 0 | consumer-sub-1 | consumer-sub-1-b97d2b45-86cf-4352-8e82-9ebdfd6fbff6 | /127.0.0.1:54214 | [test-0, test-1] |
| 1 | consumer-sub-2 | consumer-sub-2-63b7c688-3007-4650-91eb-404284dfd837 | /127.0.0.1:54213 | [test-2, test-3] |
```

Adding the `--lag` option can describe the lag info for all subscribed topic-partitions:

```bash
$ time ./build/snctl-cpp groups describe sub --lag
Group ID: sub
...
Offsets info for group 'sub' with 4 topic-partitions:
| topic-partition | committed offset | end offset | lag |
| test-0 | 0 | 0 | 0 |
| test-1 | 1 | 1 | 0 |
| test-2 | 0 | 0 | 0 |
| test-3 | 0 | 0 | 0 |
```

## Traffic

### Produce messages

Create multiple producers on a topic and split the configured total rate across
them:

```bash
$ snctl-cpp produce my-topic -n 4 --rate 1000
Started 4 producers on topic "my-topic" with total rate 1000 msg/s. Press Ctrl+C to stop.
Produced 1002 messages (1002 msg/s), failures: 0
...
```

Use `--message-size` to control the payload size in bytes. The default is 1024
bytes.

### Consume messages

Create multiple consumers on a topic:

```bash
$ snctl-cpp consume my-topic -n 4 --group my-group
Started 4 consumers on topic "my-topic" in group "my-group". Press Ctrl+C to stop.
Consumed 1000 messages (my-topic-0: 250, my-topic-1: 250, my-topic-2: 250, my-topic-3: 250), rate: 1000 msg/s, bytes: 1024000, poll errors: 0
...
```

Add `--debug` to print each consumed message's metadata:

```bash
$ snctl-cpp consume my-topic --debug
Started 1 consumer on topic "my-topic" in group "snctl-cpp-my-topic-1234567890". Press Ctrl+C to stop.
consumer[0] message topic=my-topic partition=0 offset=42 timestamp=2026-03-24 10:00:00.123 (create_time)
...
```

If `--group` is not provided, `snctl-cpp` generates one automatically. The
default offset reset policy is `earliest`, which can be changed with
`--offset-reset latest`.

## Logging

By default, rdkafka will generate logs to the standard output. `snctl-cpp` can redirect the logs to a file. For example, with the following configs in `sncloud.ini`:

```toml
[log]
enabled = true
path = /tmp/rdkafka.log
debug = broker,protocol,metadata
log_level = 7
```

The logs will be appended to the `/tmp/rdkafka.log` file. `debug` maps to
librdkafka's comma-separated debug contexts, and `log_level` maps to
librdkafka's syslog-style numeric log level. If you don't want to generate any
log from rdkafka, you can configure `enabled` with `false`.
