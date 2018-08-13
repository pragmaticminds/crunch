package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Entity for Double Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@ToString
@Entity
@DiscriminatorValue("F")
@EqualsAndHashCode
public class DoubleValue extends Value implements Serializable {

    @Column(name = "VALUE_DOUBLE")
    private Double value;

    public DoubleValue(Double value) {
        this.value = value;
    }

    public DoubleValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public Double getAsDouble() {
        return this.value;
    }

    @Override
    public void setAsDouble(Double value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public DoubleValue copy() {
        return new DoubleValue(this.value);
    }
}
