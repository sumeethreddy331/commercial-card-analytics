package com.example.card.infrastructure;

import com.example.card.application.EventPublisher;
import com.example.card.domain.CardEvent;
import com.example.card.domain.StoredEvent;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
class KafkaEventPublisher implements EventPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String topic;

    KafkaEventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json,
                        @Value("${transactions.topic}") String topic) {
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
    }

    @Bean
    NewTopic transactionTopic(@Value("${transactions.partitions}") int partitions,
                              @Value("${transactions.replicas}") int replicas) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
    }

    @Override
    public void publish(StoredEvent stored) {
        try {
            String payload = json.writeValueAsString(new Message(1, stored.ingestedAt(), stored.event()));
            kafka.send(topic, stored.event().cardToken(), payload).get(12, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new Unavailable(ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw new Unavailable(ex);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize a validated event", ex);
        }
    }

    private record Message(int schemaVersion, Instant ingestedAt, @JsonUnwrapped CardEvent event) {}
}
