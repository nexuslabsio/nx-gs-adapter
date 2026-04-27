package app.l2nx.gs.kafka;

public class NxKafkaException extends RuntimeException {

    public NxKafkaException(String message) {
        super(message);
    }

    public NxKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
