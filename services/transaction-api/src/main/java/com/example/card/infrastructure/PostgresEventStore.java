package com.example.card.infrastructure;

import com.example.card.application.EventStore;
import com.example.card.domain.CardEvent;
import com.example.card.domain.StoredEvent;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Repository
class PostgresEventStore implements EventStore {
    private static final RowMapper<StoredEvent> EVENT_ROW = (row, index) -> new StoredEvent(
            new CardEvent(row.getObject("event_id", UUID.class), row.getString("transaction_id"),
                    CardEvent.EventType.valueOf(row.getString("event_type")), row.getString("card_token"),
                    row.getString("merchant_id"), row.getString("merchant_category"),
                    row.getBigDecimal("amount"), row.getString("currency"), instant(row, "event_time"),
                    CardEvent.EventStatus.valueOf(row.getString("event_status")), row.getString("source_system")),
            instant(row, "ingested_at"), instant(row, "published_at"));

    private final JdbcClient jdbc;

    PostgresEventStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean insert(CardEvent event) {
        return jdbc.sql("""
                INSERT INTO transaction_events
                    (event_id, transaction_id, event_type, card_token, merchant_id, merchant_category,
                     amount, currency, event_time, event_status, source_system)
                VALUES (:eventId, :transactionId, :eventType, :cardToken, :merchantId, :merchantCategory,
                        :amount, :currency, :eventTime, :status, :sourceSystem)
                ON CONFLICT (event_id) DO NOTHING
                """)
                .param("eventId", event.eventId()).param("transactionId", event.transactionId())
                .param("eventType", event.eventType().name()).param("cardToken", event.cardToken())
                .param("merchantId", event.merchantId()).param("merchantCategory", event.merchantCategory())
                .param("amount", event.amount()).param("currency", event.currency())
                .param("eventTime", event.eventTime().atOffset(ZoneOffset.UTC))
                .param("status", event.status().name()).param("sourceSystem", event.sourceSystem())
                .update() == 1;
    }

    @Override
    public StoredEvent lockById(UUID eventId) {
        return jdbc.sql("SELECT * FROM transaction_events WHERE event_id = :id FOR UPDATE")
                .param("id", eventId).query(EVENT_ROW).single();
    }

    @Override
    public void markPublished(UUID eventId) {
        jdbc.sql("UPDATE transaction_events SET published_at = clock_timestamp() WHERE event_id = :id")
                .param("id", eventId).update();
    }

    @Override
    public List<StoredEvent> findByTransactionId(String transactionId) {
        return jdbc.sql("""
                SELECT * FROM transaction_events
                WHERE transaction_id = :id ORDER BY event_time, event_id
                """).param("id", transactionId).query(EVENT_ROW).list();
    }

    private static Instant instant(ResultSet row, String column) throws SQLException {
        OffsetDateTime value = row.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
