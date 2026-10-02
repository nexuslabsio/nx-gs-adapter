package app.l2nx.gs.adapter.api.kafka.events.mail;

/**
 * Which party deleted a mail. Each side holds an independent "deleted by me" flag, so one side can delete while the
 * other still sees the mail.
 */
public enum MailDeletionSide {
    SENDER,

    RECEIVER
}
