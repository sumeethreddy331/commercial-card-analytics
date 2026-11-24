package com.example.card;

import com.example.card.application.EventPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import com.example.card.domain.StoredEvent;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(kraft = true, partitions = 3, topics = IntakeIntegrationTest.TOPIC)
class IntakeIntegrationTest {
    static final String TOPIC = "commercial-card.transactions.v1";
    private static final String INTAKE = "/api/v1/transactions";
    private static EmbeddedPostgres postgres;

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired TestPublisher publisher;

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry properties) throws IOException {
        postgres = EmbeddedPostgres.builder().setServerConfig("unix_socket_directories", "").start();
        properties.add("spring.datasource.url", () -> postgres.getJdbcUrl("postgres", "postgres"));
        properties.add("spring.datasource.username", () -> "postgres");
        properties.add("spring.datasource.password", () -> "");
    }

    @AfterAll
    static void stopDatabase() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void storesPublishesAndLooksUpValidEvent() throws Exception {
        ObjectNode event = event("txn-happy");
        try (Consumer<String, String> consumer = consumer()) {
            submit(event).andExpect(status().isCreated())
                    .andExpect(header().string("Location", INTAKE + "/txn-happy"))
                    .andExpect(jsonPath("$.outcome").value("ACCEPTED"));

            ConsumerRecord<String, String> record = awaitEvent(consumer, event.path("eventId").asText());
            JsonNode message = json.readTree(record.value());
            assertThat(record.key()).isEqualTo("tok_fleet_test");
            assertThat(message.path("schemaVersion").asInt()).isEqualTo(1);
            assertThat(message.path("transactionId").asText()).isEqualTo("txn-happy");
            assertThat(message.path("eventTime").asText()).isEqualTo("2026-01-10T10:00:00Z");
            assertThat(message.path("ingestedAt").asText()).isNotBlank();
            assertThat(message.path("amount").decimalValue()).isEqualByComparingTo("72.50");
            assertThat(rowCount(event)).isEqualTo(1);

            mvc.perform(get(INTAKE + "/txn-happy")).andExpect(status().isOk())
                    .andExpect(jsonPath("$.events[0].event.eventId").value(event.path("eventId").asText()))
                    .andExpect(jsonPath("$.events[0].publicationStatus").value("PUBLISHED"));
        }
    }

    @Test
    void acceptsExactRetryAndRejectsConflictingEventId() throws Exception {
        ObjectNode event = event("txn-replay");
        try (Consumer<String, String> consumer = consumer()) {
            submit(event).andExpect(status().isCreated());
            awaitEvent(consumer, event.path("eventId").asText());
            event.put("amount", new BigDecimal("72.5"));
            event.put("eventTime", "2026-01-10T05:00:00-05:00");
            submit(event).andExpect(status().isOk()).andExpect(jsonPath("$.outcome").value("DUPLICATE"));
            event.put("amount", 90);
            submit(event).andExpect(status().isConflict())
                    .andExpect(jsonPath("$.code").value("EVENT_ID_CONFLICT"));
            assertThat(rowCount(event)).isEqualTo(1);
            assertThat(consumer.poll(Duration.ofMillis(400)).isEmpty()).isTrue();
        }
    }

    @Test
    void rejectsInvalidFieldsWithoutRecordingOrPublishing() throws Exception {
        ObjectNode event = event("txn-invalid");
        try (Consumer<String, String> consumer = consumer()) {
            event.put("amount", -3);
            event.put("cardToken", "unsupported");
            submit(event).andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("INVALID_EVENT"))
                    .andExpect(jsonPath("$.errors[?(@.field == 'amount')]").isNotEmpty())
                    .andExpect(jsonPath("$.errors[?(@.field == 'cardToken')]").isNotEmpty());
            assertThat(rowCount(event)).isZero();
            assertThat(consumer.poll(Duration.ofMillis(400)).isEmpty()).isTrue();
        }
    }

    @Test
    void reportsUnsupportedEventTypeCurrencyAndTimestamp() throws Exception {
        ObjectNode event = event("txn-contract");
        event.put("eventType", "SETTLEMENT");
        expectInvalidField(event, "eventType");
        event.put("eventType", "AUTHORIZATION");
        event.put("currency", "ZZZ");
        expectInvalidField(event, "currency");
        event.put("currency", "USD");
        event.put("eventTime", "2026-01-10T10:00:00");
        expectInvalidField(event, "eventTime");
        event.put("eventTime", 1768039200);
        expectInvalidField(event, "eventTime");
        assertThat(rowCount(event)).isZero();
    }

    @Test
    void preservesPendingEventAndRecoversOnClientRetry() throws Exception {
        ObjectNode event = event("txn-recovery");
        try (Consumer<String, String> consumer = consumer()) {
            publisher.failNext.set(true);
            submit(event).andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.code").value("PUBLISH_UNCONFIRMED"));
            mvc.perform(get(INTAKE + "/txn-recovery")).andExpect(status().isOk())
                    .andExpect(jsonPath("$.events[0].publicationStatus").value("PENDING"));
            submit(event).andExpect(status().isOk()).andExpect(jsonPath("$.outcome").value("RETRIED"));
            awaitEvent(consumer, event.path("eventId").asText());
            mvc.perform(get(INTAKE + "/txn-recovery")).andExpect(status().isOk())
                    .andExpect(jsonPath("$.events[0].publicationStatus").value("PUBLISHED"));
            assertThat(rowCount(event)).isEqualTo(1);
        }
    }

    @Test
    void handlesLateEventAndSortsHistoryByBusinessTime() throws Exception {
        ObjectNode event = event("txn-late");
        submit(event).andExpect(status().isCreated());

        ObjectNode late = event("txn-late");
        late.put("eventTime", "2026-01-09T10:00:00Z");
        submit(late).andExpect(status().isCreated());
        mvc.perform(get(INTAKE + "/txn-late")).andExpect(status().isOk())
                .andExpect(jsonPath("$.events[0].event.eventId").value(late.path("eventId").asText()))
                .andExpect(jsonPath("$.events[1].event.eventId").value(event.path("eventId").asText()));
    }

    @Test
    void fixtureProducesTheDocumentedOutcomes() throws Exception {
        List<String> fixture = Files.readAllLines(Path.of("../../fixtures/events.ndjson"));
        List<Integer> expectedCodes = List.of(201, 201, 200, 400, 201, 201, 201);
        assertThat(fixture).hasSameSizeAs(expectedCodes);
        for (int index = 0; index < fixture.size(); index++) {
            mvc.perform(post(INTAKE).contentType(APPLICATION_JSON).content(fixture.get(index)))
                    .andExpect(status().is(expectedCodes.get(index)));
        }
        mvc.perform(get(INTAKE + "/txn-001")).andExpect(status().isOk())
                .andExpect(jsonPath("$.events.length()").value(2))
                .andExpect(jsonPath("$.events[0].event.eventType").value("AUTHORIZATION"))
                .andExpect(jsonPath("$.events[1].event.eventType").value("REVERSAL"));
    }

    @Test
    void reportsMissingTransactionAndMalformedJson() throws Exception {
        mvc.perform(get(INTAKE + "/txn-missing")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TRANSACTION_NOT_FOUND"));
        mvc.perform(post(INTAKE).contentType(APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("MALFORMED_JSON"));
    }

    @TestConfiguration
    static class FailureInjection {
        @Bean
        @Primary
        TestPublisher testPublisher(@Qualifier("kafkaEventPublisher") EventPublisher delegate) {
            return new TestPublisher(delegate);
        }
    }

    static class TestPublisher implements EventPublisher {
        private final EventPublisher delegate;
        private final AtomicBoolean failNext = new AtomicBoolean();

        TestPublisher(EventPublisher delegate) {
            this.delegate = delegate;
        }

        @Override
        public void publish(StoredEvent event) {
            if (failNext.getAndSet(false)) {
                throw new Unavailable(new IOException("simulated outage"));
            }
            delegate.publish(event);
        }
    }

    private ObjectNode event(String transactionId) {
        return json.createObjectNode()
                .put("eventId", UUID.randomUUID().toString()).put("transactionId", transactionId)
                .put("eventType", "AUTHORIZATION").put("cardToken", "tok_fleet_test")
                .put("merchantId", "shop-test").put("merchantCategory", "FUEL")
                .put("amount", new BigDecimal("72.50")).put("currency", "USD")
                .put("eventTime", "2026-01-10T10:00:00Z").put("status", "APPROVED")
                .put("sourceSystem", "synthetic-test");
    }

    private ResultActions submit(ObjectNode event) throws Exception {
        return mvc.perform(post(INTAKE).contentType(APPLICATION_JSON).content(json.writeValueAsBytes(event)));
    }

    private void expectInvalidField(ObjectNode event, String field) throws Exception {
        submit(event).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value(field));
    }

    private Integer rowCount(ObjectNode event) {
        return jdbc.queryForObject("SELECT count(*) FROM transaction_events WHERE event_id = ?", Integer.class,
                UUID.fromString(event.path("eventId").asText()));
    }

    private Consumer<String, String> consumer() {
        Map<String, Object> properties = KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "false", broker);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                properties, new StringDeserializer(), new StringDeserializer()).createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, true, TOPIC);
        return consumer;
    }

    private ConsumerRecord<String, String> awaitEvent(Consumer<String, String> consumer, String eventId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                if (json.readTree(record.value()).path("eventId").asText().equals(eventId)) {
                    return record;
                }
            }
        }
        throw new AssertionError("Kafka event was not received: " + eventId);
    }
}
