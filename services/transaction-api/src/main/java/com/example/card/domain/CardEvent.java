package com.example.card.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record CardEvent(
        UUID eventId,
        String transactionId,
        EventType eventType,
        String cardToken,
        String merchantId,
        String merchantCategory,
        BigDecimal amount,
        String currency,
        Instant eventTime,
        EventStatus status,
        String sourceSystem
) {
    public enum EventType { AUTHORIZATION, REVERSAL, ADJUSTMENT }
    public enum EventStatus { APPROVED, DECLINED, REVERSED }
}
