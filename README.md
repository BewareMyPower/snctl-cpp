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
[0] my-group Stable
```

The format of each line is `[index] <group-id> <state>`.

## Logging

By default, rdkafka will generate logs to the standard output. `snctl-cpp` can redirect the logs to a file. For example, with the following configs in `sncloud.ini`:

```toml
[log]
enabled = true
path = /tmp/rdkafka.log
```

The logs will be appended to the `/tmp/rdkafka.log` file. If you don't want to generate any log from rdkafka, you can configure `enabled` with `false`.
