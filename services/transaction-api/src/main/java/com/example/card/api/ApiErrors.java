package com.example.card.api;

import com.example.card.application.EventPublisher;
import com.example.card.application.TransactionIntake;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
class ApiErrors {
    record FieldError(String field, String message) {}
    record ErrorBody(String code, String message, List<FieldError> errors) {}

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorBody> validation(MethodArgumentNotValidException ex) {
        List<FieldError> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> new FieldError(error.getField(), error.getDefaultMessage())).distinct().toList();
        return response(HttpStatus.BAD_REQUEST, "INVALID_EVENT", "Check the event fields.", errors);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ErrorBody> malformed(HttpMessageNotReadableException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof UnrecognizedPropertyException) {
            return response(HttpStatus.BAD_REQUEST, "INVALID_EVENT", "Use only fields in the v1 contract.",
                    List.of(new FieldError("body", "contains an unsupported field")));
        }
        if (cause instanceof JsonMappingException mapping && !mapping.getPath().isEmpty()) {
            String field = mapping.getPath().getFirst().getFieldName();
            String message = Map.of("eventId", "must be a UUID",
                    "eventTime", "must be an ISO-8601 timestamp with a UTC offset",
                    "amount", "must be a JSON decimal number").getOrDefault(field, "has an invalid value or type");
            if (cause instanceof InvalidFormatException invalid && invalid.getTargetType().isEnum()) {
                message = "must be one of " + Arrays.toString(invalid.getTargetType().getEnumConstants());
            }
            return invalid(new InvalidField(field, message));
        }
        return response(HttpStatus.BAD_REQUEST, "MALFORMED_JSON", "Send a valid JSON event object.", List.of());
    }

    @ExceptionHandler(InvalidField.class)
    ResponseEntity<ErrorBody> invalid(InvalidField ex) {
        return response(HttpStatus.BAD_REQUEST, "INVALID_EVENT", "Check the event fields.",
                List.of(new FieldError(ex.field, ex.getMessage())));
    }

    @ExceptionHandler(TransactionIntake.EventConflict.class)
    ResponseEntity<ErrorBody> conflict() {
        return response(HttpStatus.CONFLICT, "EVENT_ID_CONFLICT",
                "This eventId is already stored with a different payload.", List.of());
    }

    @ExceptionHandler(TransactionController.UnknownTransaction.class)
    ResponseEntity<ErrorBody> notFound() {
        return response(HttpStatus.NOT_FOUND, "TRANSACTION_NOT_FOUND",
                "No events were found for that transactionId.", List.of());
    }

    @ExceptionHandler(EventPublisher.Unavailable.class)
    ResponseEntity<ErrorBody> publicationUnavailable() {
        return response(HttpStatus.SERVICE_UNAVAILABLE, "PUBLISH_UNCONFIRMED",
                "Event recorded. Retry the same eventId and payload to confirm publication.", List.of());
    }

    @ExceptionHandler({DataAccessException.class, TransactionException.class})
    ResponseEntity<ErrorBody> databaseUnavailable() {
        return response(HttpStatus.SERVICE_UNAVAILABLE, "STORAGE_ERROR",
                "Could not complete the operation. Retry using the original eventId for intake.", List.of());
    }

    private ResponseEntity<ErrorBody> response(HttpStatus status, String code, String message,
                                               List<FieldError> errors) {
        return ResponseEntity.status(status).body(new ErrorBody(code, message, errors));
    }

    static final class InvalidField extends RuntimeException {
        private final String field;

        InvalidField(String field, String message) {
            super(message);
            this.field = field;
        }
    }
}
