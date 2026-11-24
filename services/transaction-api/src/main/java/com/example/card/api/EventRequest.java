package com.example.card.api;

import com.example.card.domain.CardEvent;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Currency;
import java.util.UUID;

public record EventRequest(
        @NotNull UUID eventId,
        @NotBlank @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String transactionId,
        @NotNull CardEvent.EventType eventType,
        @NotBlank @Pattern(regexp = "tok_[A-Za-z0-9_-]{1,60}",
                message = "must be a synthetic token beginning with tok_") String cardToken,
        @NotBlank @Size(max = 64) String merchantId,
        @NotBlank @Size(max = 64) String merchantCategory,
        @NotNull @DecimalMin("0.01") @Digits(integer = 16, fraction = 2) BigDecimal amount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotBlank String eventTime,
        @NotNull CardEvent.EventStatus status,
        @NotBlank @Size(max = 64) String sourceSystem
) {
    CardEvent toEvent() {
        try {
            Currency.getInstance(currency);
        } catch (IllegalArgumentException ex) {
            throw new ApiErrors.InvalidField("currency", "must be a recognized ISO 4217 code");
        }
        Instant timestamp = parseTimestamp();
        int utcYear = timestamp.atOffset(java.time.ZoneOffset.UTC).getYear();
        if (utcYear < 1 || utcYear > 9999 || timestamp.getNano() % 1000 != 0) {
            throw new ApiErrors.InvalidField("eventTime",
                    "must use years 0001 through 9999 and at most microsecond precision");
        }
        return new CardEvent(eventId, transactionId, eventType, cardToken, merchantId,
                merchantCategory, amount.setScale(2), currency, timestamp, status, sourceSystem);
    }

    private Instant parseTimestamp() {
        try {
            return OffsetDateTime.parse(eventTime).toInstant();
        } catch (DateTimeParseException ex) {
            throw new ApiErrors.InvalidField("eventTime", "must be an ISO-8601 timestamp with a UTC offset");
        }
    }
}
