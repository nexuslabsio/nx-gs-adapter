package app.l2nx.gs.adapter.api.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KafkaCredentialsTest {

    @Test
    void toString_shouldRedactSaslPassword() {
        KafkaCredentials kafka = KafkaCredentials.builder()
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
