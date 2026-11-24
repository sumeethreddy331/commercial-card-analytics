# Repository instructions

Use Java 21 and Spring Boot for the transaction API in `services/transaction-api`. Keep HTTP contracts under `api`, persistence and Kafka adapters under `infrastructure`, and business flow under `application` and `domain`. Keep Compose dependencies in `infra`, synthetic inputs in `fixtures`, and contracts in `docs`. Future pipeline and analytics modules should have their own directories.

Follow the README startup steps. Run `mvn verify` before proposing a change. When modifying a versioned contract, update its documentation and exercise valid, invalid, and replay behavior. Add new Flyway migrations for schema changes after V1.

Use synthetic identifiers and values. Keep credentials, real cardholder details, payment authentication data and request payloads out of logs, fixtures, prompts and commits.

Review AI-assisted changes against the assignment, the event contract, SQL precision and timestamp mappings, Kafka keys, retry behavior and test results. Confirm each claimed check ran before updating the completion note. Disclose where AI assisted and flag checks that still require manual review.
