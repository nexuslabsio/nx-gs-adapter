package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for how a character looks, carried in {@link CharacterDbDto#getDisplay()}. Every field is null
 * when the provider cannot read it; a value outside the creation range is sent as null, never clamped.
 */
public final class CharacterDisplayDbDto {

    private final @Nullable CharacterRace race;
    private final @Nullable CharacterSex sex;
    private final @Nullable Integer face;
    private final @Nullable Integer hairStyle;
    private final @Nullable Integer hairColor;
    private final @Nullable Integer nameColor;
    private final @Nullable Integer titleColor;

    public CharacterDisplayDbDto(
            @Nullable CharacterRace race,
            @Nullable CharacterSex sex,
            @Nullable Integer face,
            @Nullable Integer hairStyle,
            @Nullable Integer hairColor,
            @Nullable Integer nameColor,
            @Nullable Integer titleColor) {
        this.race = race;
        this.sex = sex;
        this.face = face;
        this.hairStyle = hairStyle;
        this.hairColor = hairColor;
        this.nameColor = nameColor;
        this.titleColor = titleColor;
    }

    public @Nullable CharacterRace getRace() {
        return race;
    }

    public @Nullable CharacterSex getSex() {
        return sex;
    }

    /** 0-based. */
    public @Nullable Integer getFace() {
        return face;
    }

    /** 0-based; the range depends on sex. */
    public @Nullable Integer getHairStyle() {
        return hairStyle;
    }

    /** 0-based. */
    public @Nullable Integer getHairColor() {
        return hairColor;
    }

    /**
     * Persistent name color, RGB {@code 0xRRGGBB}; null when unset and the client default applies. Colors
     * the server overrides at render time (PvP/PK, nobility, offline trade, access level) are not reported.
     */
    public @Nullable Integer getNameColor() {
        return nameColor;
    }

    /** Title color, same semantics as {@link #getNameColor()}. */
    public @Nullable Integer getTitleColor() {
        return titleColor;
    }

    public Builder toBuilder() {
        return new Builder()
                .race(race)
                .sex(sex)
                .face(face)
                .hairStyle(hairStyle)
                .hairColor(hairColor)
                .nameColor(nameColor)
                .titleColor(titleColor);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterDisplayDbDto)) return false;
        CharacterDisplayDbDto that = (CharacterDisplayDbDto) o;
        return race == that.race
                && sex == that.sex
                && Objects.equals(face, that.face)
                && Objects.equals(hairStyle, that.hairStyle)
                && Objects.equals(hairColor, that.hairColor)
                && Objects.equals(nameColor, that.nameColor)
                && Objects.equals(titleColor, that.titleColor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(race, sex, face, hairStyle, hairColor, nameColor, titleColor);
    }

    @Override
    public String toString() {
        return "CharacterDisplayDbDto[race=" + race
                + ", sex=" + sex
                + ", face=" + face
                + ", hairStyle=" + hairStyle
                + ", hairColor=" + hairColor
                + ", nameColor=" + nameColor
                + ", titleColor=" + titleColor + "]";
    }

    public static final class Builder {
        private @Nullable CharacterRace race;
        private @Nullable CharacterSex sex;
        private @Nullable Integer face;
        private @Nullable Integer hairStyle;
        private @Nullable Integer hairColor;
        private @Nullable Integer nameColor;
        private @Nullable Integer titleColor;

        public Builder race(@Nullable CharacterRace race) {
            this.race = race;
            return this;
        }

        public Builder sex(@Nullable CharacterSex sex) {
            this.sex = sex;
            return this;
        }

        public Builder face(@Nullable Integer face) {
            this.face = face;
            return this;
        }

        public Builder hairStyle(@Nullable Integer hairStyle) {
            this.hairStyle = hairStyle;
            return this;
        }

        public Builder hairColor(@Nullable Integer hairColor) {
            this.hairColor = hairColor;
            return this;
        }

        public Builder nameColor(@Nullable Integer nameColor) {
            this.nameColor = nameColor;
            return this;
        }

        public Builder titleColor(@Nullable Integer titleColor) {
            this.titleColor = titleColor;
            return this;
        }

        public CharacterDisplayDbDto build() {
            return new CharacterDisplayDbDto(race, sex, face, hairStyle, hairColor, nameColor, titleColor);
        }
    }
}
