package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

public enum CharacterEffectOffline {
    /** Timer stands still while offline and resumes on login. */
    FROZEN,
    /** Keeps running offline and may expire before the next login. */
    TICKING,
    DROPPED
}
