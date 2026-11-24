package com.example.card.domain;

import java.time.Instant;

public record StoredEvent(CardEvent event, Instant ingestedAt, Instant publishedAt) {
    public boolean isPublished() {
        return publishedAt != null;
    }
}
