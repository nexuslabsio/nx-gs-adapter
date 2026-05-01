package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;

/**
 * Kafka credentials and bootstrap addressing returned in {@link ConnectResponse}.
 *
 * <p>{@code saslPassword} travels in plaintext within this DTO — wire-level confidentiality
 * is the transport's responsibility, not this contract's.</p>
 *
 * <p>{@link #toString()} redacts {@code saslPassword}.</p>
 */
public final class KafkaConfig {

    private final String bootstrap;
    private final String securityProtocol;
    private final String saslMechanism;
    private final String saslUsername;
    private final String saslPassword;

    public KafkaConfig(String bootstrap,
                       String securityProtocol,
                       String saslMechanism,
                       String saslUsername,
                       String saslPassword) {
        this.bootstrap = bootstrap;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSaslUsername() {
        return saslUsername;
    }

    public String getSaslPassword() {
        return saslPassword;
    }

    public Builder toBuilder() {
        return new Builder()
                .bootstrap(bootstrap)
                .securityProtocol(securityProtocol)
                .saslMechanism(saslMechanism)
                .saslUsername(saslUsername)
                .saslPassword(saslPassword);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaConfig)) return false;
        KafkaConfig that = (KafkaConfig) o;
        return Objects.equals(bootstrap, that.bootstrap)
                && Objects.equals(securityProtocol, that.securityProtocol)
                && Objects.equals(saslMechanism, that.saslMechanism)
                && Objects.equals(saslUsername, that.saslUsername)
                && Objects.equals(saslPassword, that.saslPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrap, securityProtocol, saslMechanism, saslUsername, saslPassword);
    }

    @Override
    public String toString() {
        return "KafkaConfig[bootstrap=" + bootstrap
                + ", securityProtocol=" + securityProtocol
                + ", saslMechanism=" + saslMechanism
                + ", saslUsername=" + saslUsername
                + ", saslPassword=***]";
    }

    public static final class Builder {
        private String bootstrap;
        private String securityProtocol;
        private String saslMechanism;
        private String saslUsername;
        private String saslPassword;

        public Builder bootstrap(String bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }

        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder saslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }

        public Builder saslUsername(String saslUsername) {
            this.saslUsername = saslUsername;
            return this;
        }

        public Builder saslPassword(String saslPassword) {
            this.saslPassword = saslPassword;
            return this;
        }

        public KafkaConfig build() {
            return new KafkaConfig(bootstrap, securityProtocol, saslMechanism, saslUsername, saslPassword);
        }
    }
}
