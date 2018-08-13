package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Entity for Boolean Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@DiscriminatorValue("B")
@ToString
@EqualsAndHashCode
public class BooleanValue extends Value implements Serializable {

    @Column(name = "VALUE_BOOL")
    private Boolean value;

    public BooleanValue(Boolean value) {
        this.value = value;
    }

    public BooleanValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public boolean getAsBoolean() {
        return value;
    }

    @Override
    public void setAsBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public BooleanValue copy() {
        return new BooleanValue(this.value);
    }
}
