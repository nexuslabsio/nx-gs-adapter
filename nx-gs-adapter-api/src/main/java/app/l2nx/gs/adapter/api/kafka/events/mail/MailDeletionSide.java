package app.l2nx.gs.adapter.api.kafka.events.mail;

/**
 * Which party deleted a mail. The two sides are orthogonal in the host —
 * sender and receiver each hold an independent "deleted by me" flag, so the
 * same mail can be deleted by one side while still visible to the other.
 */
public enum MailDeletionSide {

    /**
     * The sender removed the mail from their outbox
     * (source {@code _deletedBySender}).
     */
    SENDER,

    /**
     * The receiver removed the mail from their inbox
     * (source {@code _deletedByReceiver}).
     */
    RECEIVER
}
