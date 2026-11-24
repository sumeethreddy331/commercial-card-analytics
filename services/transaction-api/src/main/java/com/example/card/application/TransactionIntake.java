package com.example.card.application;

import com.example.card.domain.CardEvent;
import com.example.card.domain.StoredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

@Service
public class TransactionIntake {
    private static final Logger log = LoggerFactory.getLogger(TransactionIntake.class);

    private final EventStore store;
    private final EventPublisher publisher;
    private final TransactionTemplate transactions;

    public TransactionIntake(EventStore store, EventPublisher publisher, TransactionTemplate transactions) {
        this.store = store;
        this.publisher = publisher;
        this.transactions = transactions;
    }

    public Result accept(CardEvent event) {
        // Commit receipt first so a publish failure leaves a durable, retryable event.
        boolean created = store.insert(event);
        try {
            return transactions.execute(status -> {
                StoredEvent stored = store.lockById(event.eventId());
                if (!stored.event().equals(event)) {
                    throw new EventConflict();
                }
                if (stored.isPublished()) {
                    return Result.DUPLICATE;
                }
                publisher.publish(stored);
                store.markPublished(event.eventId());
                return created ? Result.ACCEPTED : Result.RETRIED;
            });
        } catch (EventPublisher.Unavailable ex) {
            log.atWarn().addKeyValue("eventId", event.eventId())
                    .addKeyValue("outcome", "PUBLISH_UNCONFIRMED").log("Kafka publication unconfirmed");
            throw ex;
        }
    }

    public List<StoredEvent> lookup(String transactionId) {
        return store.findByTransactionId(transactionId);
    }

    public enum Result { ACCEPTED, DUPLICATE, RETRIED }

    public static final class EventConflict extends RuntimeException {}
}
