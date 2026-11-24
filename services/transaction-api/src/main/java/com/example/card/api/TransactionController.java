package com.example.card.api;

import com.example.card.application.TransactionIntake;
import com.example.card.domain.CardEvent;
import com.example.card.domain.StoredEvent;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transactions")
class TransactionController {
    private final TransactionIntake intake;

    TransactionController(TransactionIntake intake) {
        this.intake = intake;
    }

    @PostMapping
    ResponseEntity<Receipt> receive(@Valid @RequestBody EventRequest request) {
        CardEvent event = request.toEvent();
        TransactionIntake.Result result = intake.accept(event);
        Receipt receipt = new Receipt(event.eventId(), event.transactionId(), result);
        if (result == TransactionIntake.Result.ACCEPTED) {
            return ResponseEntity.created(UriComponentsBuilder.fromPath("/api/v1/transactions/{id}")
                    .buildAndExpand(event.transactionId()).toUri()).body(receipt);
        }
        return ResponseEntity.ok(receipt);
    }

    @GetMapping("/{transactionId}")
    TransactionHistory lookup(@PathVariable String transactionId) {
        if (!transactionId.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new ApiErrors.InvalidField("transactionId", "must contain 1 to 64 letters, digits, underscores or hyphens");
        }
        List<EventHistory> events = intake.lookup(transactionId).stream().map(EventHistory::from).toList();
        if (events.isEmpty()) {
            throw new UnknownTransaction();
        }
        return new TransactionHistory(transactionId, events);
    }

    record Receipt(UUID eventId, String transactionId, TransactionIntake.Result outcome) {}
    record TransactionHistory(String transactionId, List<EventHistory> events) {}

    record EventHistory(CardEvent event, Instant ingestedAt, Instant publishedAt, String publicationStatus) {
        static EventHistory from(StoredEvent stored) {
            return new EventHistory(stored.event(), stored.ingestedAt(), stored.publishedAt(),
                    stored.isPublished() ? "PUBLISHED" : "PENDING");
        }
    }

    static final class UnknownTransaction extends RuntimeException {}
}
