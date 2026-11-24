package com.example.card.application;

import com.example.card.domain.CardEvent;
import com.example.card.domain.StoredEvent;

import java.util.List;
import java.util.UUID;

public interface EventStore {
    boolean insert(CardEvent event);
    StoredEvent lockById(UUID eventId);
    void markPublished(UUID eventId);
    List<StoredEvent> findByTransactionId(String transactionId);
}
