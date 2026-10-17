# Rockbee CDC

CDC means **Change Data Capture**. This optional tool uses SlateDB's WAL reader
to read durable PUT and DELETE changes from object storage. It prints one JSON
event per line. Rockbee readers work without this tool.

```text
RockBeeClient -> Gateway -> Writer -> SlateDB -> S3 WAL
                                           |
                                           v
                                  rockbee-cdc -> JSON
```

## Run locally

Follow the [local setup](../../README.md#try-it-locally) first. Run all commands
below from the repository root.

For the default `make writer`, `make reader`, and `make gateway` setup:

```sh
./target/release/rockbee-cdc \
  --backend localstack \
  --endpoint http://127.0.0.1:4566 \
  --bucket rockbee-test \
  --prefix rockbee-dev \
  --cursor ./rockbee-cdc.cursor.json
```

Use the **same endpoint, bucket, and prefix as the writer**. If you started with
`make local`, use the `prefix` and `gateway` values from its `ready` output instead
of `rockbee-dev` and `127.0.0.1:7100` below.

In another terminal:

```sh
./target/release/rockbee-client --addr 127.0.0.1:7100 put k v
./target/release/rockbee-client --addr 127.0.0.1:7100 delete k
```

The CDC terminal shows events like these; sequence and WAL numbers may differ:

```json
{"op":"put","key":[107],"value":[118],"seq":1,"wal_file_id":2}
{"op":"delete","key":[107],"value":null,"seq":2,"wal_file_id":3}
```

Keys and values are byte arrays: `107` is `k`, and `118` is `v`. `seq` is the
database sequence number; `wal_file_id` identifies the WAL file.

## Stop and resume

Press **Ctrl+C** to stop. Run the same command with the same cursor file to
continue after the last completed WAL file. When caught up, the tool waits for
new changes.

## Keep in mind

- A new cursor starts at WAL file 1. If a required WAL was already deleted, the
  tool stops with `resnapshot is required`. It does not create a full data copy.
- A crash can cause repeated events. Consumers must handle duplicates. Writing
  to stdout does not confirm that another system has saved the event.
- TTL PUTs include `expires_at_unix_ms`. Expiration does not emit a DELETE event;
  the consumer must check that timestamp.
- Output goes to stdout. Sending it to Kafka or another service needs a separate
  consumer.
