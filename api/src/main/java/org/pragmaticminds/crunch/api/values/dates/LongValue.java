package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

/**
 * Entity for Long Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@DiscriminatorValue("L")
@ToString
@EqualsAndHashCode
public class LongValue extends Value implements Serializable {

    @Column(name = "VALUE_LONG")
    private Long value;

    public LongValue(Long value) {
        this.value = value;
    }

    public LongValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public Long getAsLong() {
        return value;
    }

    @Override
    public void setAsLong(Long value) {
        this.value = value;
    }

    @Override
    public String getAsString() {
        return Long.toString(value);
    }

    @Override
    public Double getAsDouble() {
        return (double) value;
    }

    @Override
    public Date getAsDate() {
        return Date.from(Instant.ofEpochMilli(this.value));
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Value copy() {
        return new LongValue(this.value);
    }
}
