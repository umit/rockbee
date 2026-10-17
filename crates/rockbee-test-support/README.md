# Rockbee test support

Development-only shared integration fixtures. `Store` dispatches between the
concrete writer and reader application libraries so regressions can exercise both
roles with the same harness. Production packages must only use this crate under
`dev-dependencies`.

```text
integration tests -> Store fixture -> Writer | Reader -> SlateDB
```

The migrated durability, TTL, binary protocol, memory, and eventual-convergence
suites live here. Executable startup/lifecycle tests live with their role apps.
The opt-in LocalStack test requires existing infrastructure and unique prefixes.
