package app.l2nx.gs.adapter.api.rest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConfigTest {

    @Test
    void toString_shouldRedactSaslPassword() {
        KafkaConfig kafka = KafkaConfig.builder()
                .bootstrap("kafka.l2nx.online:9094")
                .securityProtocol("SASL_SSL")
                .saslMechanism("SCRAM-SHA-512")
                .saslUsername("acme")
                .saslPassword("S3cureP@ss!")
                .build();

        String rendered = kafka.toString();

        assertFalse(rendered.contains("S3cureP@ss!"), "rendered=" + rendered);
        assertTrue(rendered.contains("***"));
        assertTrue(rendered.contains("saslUsername=acme"));
    }
}
