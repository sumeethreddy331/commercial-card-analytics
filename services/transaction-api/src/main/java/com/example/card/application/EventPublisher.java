package com.example.card.application;

import com.example.card.domain.StoredEvent;

public interface EventPublisher {
    void publish(StoredEvent event);

    class Unavailable extends RuntimeException {
        public Unavailable(Throwable cause) {
            super(cause);
        }
    }
}
