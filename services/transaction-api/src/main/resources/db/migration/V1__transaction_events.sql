CREATE TABLE transaction_events (
    event_id uuid PRIMARY KEY,
    transaction_id varchar(64) NOT NULL,
    event_type varchar(16) NOT NULL CHECK (event_type IN ('AUTHORIZATION', 'REVERSAL', 'ADJUSTMENT')),
    card_token varchar(64) NOT NULL,
    merchant_id varchar(64) NOT NULL,
    merchant_category varchar(64) NOT NULL,
    amount numeric(18, 2) NOT NULL CHECK (amount > 0),
    currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    event_time timestamptz NOT NULL,
    event_status varchar(16) NOT NULL CHECK (event_status IN ('APPROVED', 'DECLINED', 'REVERSED')),
    source_system varchar(64) NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);

CREATE INDEX transaction_events_lookup_idx ON transaction_events (transaction_id, event_time, event_id);
