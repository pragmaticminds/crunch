package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

/**
 * Entity for Date Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@DiscriminatorValue("D")
@ToString
@EqualsAndHashCode
public class DateValue extends Value implements Serializable {

    @Column(name = "VALUE_DATE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date value;

    public DateValue(Date value) {
        this.value = value;
    }

    public DateValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public Date getAsDate() {
        return value;
    }

    @Override
    public void setAsDate(Date value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public DateValue copy() {
        return new DateValue(this.value);
    }
}
