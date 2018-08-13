package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Entity for String Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@ToString
@DiscriminatorValue("S")
@EqualsAndHashCode
public class StringValue extends Value implements Serializable {

    @Column(name = "VALUE_STRING", length = 8096)
    private String value;

    public StringValue() {
        //for jpa
    }

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public String getAsString() {
        return value;
    }

    @Override
    public void setAsString(String value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public StringValue copy() {
        return new StringValue(this.value);
    }
}
