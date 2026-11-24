# Commercial Card Transaction Processing and Analytics

This Day 1 milestone implements transaction intake and lookup, PostgreSQL persistence, and Kafka publication. The remaining assignment covers the Bronze, Silver, and Gold pipeline, the analytics API, and the final demo.

## Start locally

Install JDK 21, Maven 3.9+, Docker with Compose v2, and curl. From the repository root, run this in a Bash-compatible shell. Use a password reserved for this local instance.

```bash
read -rsp "Local PostgreSQL password: " TRANSACTIONS_DB_PASSWORD
export TRANSACTIONS_DB_PASSWORD
printf '\n'
docker compose -f infra/compose.yaml up -d --wait postgres kafka
mvn -pl services/transaction-api spring-boot:run
```

Spring Boot applies the Flyway migration and provisions the Kafka topic on startup. Stop the Java process with Ctrl+C and run `docker compose -f infra/compose.yaml down` to stop dependencies. Named volumes retain the database and Kafka log. An optional `lake` Compose profile defines MinIO for the later pipeline phase. Set local-only `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` before starting it.

## Demo the intake milestone

Use a fresh local stack for the exact outcomes below. In a second shell, from the repo root, submit the fixture and then retrieve the transaction's history.

```bash
while IFS= read -r event; do
  curl -sS -w '\nHTTP %{http_code}\n' \
    -H 'Content-Type: application/json' --data-binary "$event" \
    http://localhost:8080/api/v1/transactions
done < fixtures/events.ndjson

curl -fsS http://localhost:8080/api/v1/transactions/txn-001
```

| Fixture row | Case | Expected intake result |
|---|---|---|
| 1 | Approved authorization | 201 ACCEPTED |
| 2 | Declined authorization | 201 ACCEPTED |
| 3 | Identical event ID and payload | 200 DUPLICATE, with no second Kafka send |
| 4 | Negative amount | 400 INVALID_EVENT, with an amount error |
| 5 | Reversal | 201 ACCEPTED |
| 6 | Older event time received after newer records | 201 ACCEPTED |
| 7 | Adjustment | 201 ACCEPTED |

Lookup for `txn-001` returns the authorization followed by the reversal, with the business event, ingestion time and publication status for each. Lookup results are ordered by business event time and then by event ID. Replaying the fixture preserves the five accepted rows.

Read the five Kafka records on a fresh stack from the broker container. Each line shows the synthetic card key followed by the versioned JSON value. A timeout will end the command if five records are unavailable.

```bash
docker compose -f infra/compose.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic commercial-card.transactions.v1 \
  --from-beginning --property print.key=true --max-messages 5 --timeout-ms 10000
```

## Tests

Run `mvn verify` from the repository root. The suite uses an embedded Kafka broker and native PostgreSQL binaries, with no Docker dependency. Run it under a regular user on a platform supported by the embedded PostgreSQL test library. The tests exercise the real Flyway migration, SQL, and Kafka producer through the Spring MVC request path.

## Troubleshooting

Check `docker compose -f infra/compose.yaml ps` and `docker compose -f infra/compose.yaml logs postgres kafka` for startup failures. Host ports 5432 and 19092 must be available. Changing the password environment variable does not reset a PostgreSQL user in an existing named volume. A 503 with `PUBLISH_UNCONFIRMED` means the database row exists. Restore the broker and submit the identical payload to retry. See the [architecture and event contract](docs/architecture.md) for failure semantics.
