# Command-line client

The default gateway is `127.0.0.1:7100`; put `--addr HOST:PORT` before the command
to use another address. Existing byte commands are `put KEY VALUE [--ttl-ms N]`,
`get KEY`, and `delete KEY`. `status` prints the contacted service's local status.

JSON table commands accept a JSON object and SQL as single quoted shell arguments:

```sh
rockbee-client table-put users '{"name":"Ada","age":36}'
rockbee-client table-put users '{"name":"Ada","age":37}' --id ada --ttl-ms 60000
rockbee-client table-get users ada
rockbee-client table-query users "SELECT value->>'name' AS name, TRY_CAST(value->>'age' AS BIGINT) AS age FROM users ORDER BY age"
rockbee-client table-delete users ada
```

PUT prints `{"id":"..."}` after durable acknowledgement, generating a UUID when
`--id` is omitted. TTL zero means no expiry. GET prints the JSON object or `null`.
DELETE prints `{"status":"ok"}`. Queries print `rows` and the
`exported_wal_file_id` watermark. Mutations are never automatically replayed.

```text
CLI -> RockBeeClient -> Gateway -- writes --> Writer -> SlateDB WAL
                       -- reads ---> Reader <- exported Parquet
```

Readers converge eventually: a successful PUT does not guarantee its immediate
visibility to table GET or SQL, and the export watermark can lag the writer.
