/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.api.values.dates;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.events.UntypedEvent;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;

/**
 * Entities to store single "values" with different types.
 * In Jpa Inheritance has to be used.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@ToString
@EqualsAndHashCode
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BooleanValue.class, name = "bool"),
        @JsonSubTypes.Type(value = DateValue.class, name = "date"),
        @JsonSubTypes.Type(value = DoubleValue.class, name = "double"),
        @JsonSubTypes.Type(value = LongValue.class, name = "long"),
        @JsonSubTypes.Type(value = StringValue.class, name = "string"),
})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class Value implements Serializable {
    private static final double DOUBLE_ERROR_VALUE = -99999.999;

    // Used for JPA in Server.
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    // Static Constructor Methods
    public static Value of(String s) {
        return new StringValue(s);
    }

    public static Value of(Date d) {
        return new DateValue(d);
    }

    public static Value of(double d) {
        return new DoubleValue(d);
    }

    public static Value of(long l) {
        return new LongValue(l);
    }

    public static Value of(boolean b) {
        return new BooleanValue(b);
    }

    public static Value of(int i) {
        return new LongValue((long) i);
    }

    /**
     * checks if the Object is assignable to one of the classes
     * Boolean, Date, Double, Long or String and creates a Value object which is returned. If this is not possible
     * a ClassCastException will be thrown
     *
     * @param o the Object to be transformed into a Value object
     * @return the Value the element of the map at the given key is transformed to
     */
    public static Value of(Object o) {
        if (o == null){
            return null;
        }
        // Itempotency
        if (Value.class.isAssignableFrom(o.getClass())) {
            return (Value) o;
        }
        if (Boolean.class.isAssignableFrom(o.getClass())) {
            return new BooleanValue((Boolean) o);
        }
        if (Date.class.isAssignableFrom(o.getClass())) {
            return new DateValue((Date) o);
        }
        if (Double.class.isAssignableFrom(o.getClass())) {
            return new DoubleValue((Double) o);
        }
        if (Long.class.isAssignableFrom(o.getClass())) {
            return new LongValue((Long) o);
        }
        if (Integer.class.isAssignableFrom(o.getClass())) {
            return new LongValue((long) (Integer) o);
        }
        if (String.class.isAssignableFrom(o.getClass())) {
            return new StringValue((String) o);
        }
        if (UntypedEvent.SerializableDate.class.isAssignableFrom(o.getClass())) {
            return new DateValue(((UntypedEvent.SerializableDate) o).asDate());
        }
        // fail detection of UntypedEvent.SerializableDate as LinkedHashMap
        if (LinkedHashMap.class.isAssignableFrom(o.getClass()) &&
                ((LinkedHashMap) o).values().toArray().length == 1) {
            return new DateValue(Date.from(Instant.ofEpochMilli(
                    (long) ((LinkedHashMap) o).values().toArray()[0]
            )));
        }
        if (LinkedHashMap.class.isAssignableFrom(o.getClass())) {
            return of(((LinkedHashMap) o).values().toArray()[1]
            );
        }

        throw new InvalidParameterException("Not able to cast " + o + " with Type " + o.getClass());
    }

    @JsonIgnore
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @JsonIgnore
    public abstract Object getAsObject();

    @JsonIgnore
    public Long getAsLong() {
        throw new UnsupportedOperationException("Cannot getValue Long Value");
    }

    public void setAsLong(Long value) {
        throw new UnsupportedOperationException("Cannot set Long Value");
    }

    @JsonIgnore
    public String getAsString() {
        return toString();
    }

    public void setAsString(String value) {
        throw new UnsupportedOperationException("Cannot set String Value");
    }

    @JsonIgnore
    public Double getAsDouble() {
        //TM 15.07.2018 fixing flink problems
        //returning a constant value of -99999.999 instead of an exception
        return DOUBLE_ERROR_VALUE;
    }

    public void setAsDouble(Double value) {
        throw new UnsupportedOperationException("Cannot set Double Value");
    }

    @JsonIgnore
    public Date getAsDate() {
        throw new UnsupportedOperationException("Cannot getValue Date Value");
    }

    public void setAsDate(Date value) {
        throw new UnsupportedOperationException("Cannot set Date Value");
    }

    @JsonIgnore
    public boolean getAsBoolean() {
        throw new UnsupportedOperationException("Cannot getValue Boolean Value");
    }

    public void setAsBoolean(boolean value) {
        throw new UnsupportedOperationException("Cannot set Boolean Value");
    }

    public abstract Value copy();

    public abstract <T> T accept(ValueVisitor<T> visitor);

    /**
     * Returns the requested value in the requested DataType (if possible).
     *
     * @param dataType DataType to transform to
     * @return
     * @throws UnsupportedOperationException if the cast cannot be done
     */
    public Object getAsDataType(DataType dataType) {
        switch (dataType) {
            case BOOL:
                return this.getAsBoolean();
            case BYTE:
            case INT_32:
            case LONG:
                return this.getAsLong();
            case DOUBLE:
                return this.getAsDouble();
            case STRING:
                return this.getAsString();
            case TIMESTAMP:
                return this.getAsDate().toInstant();
            default:
                throw new UnsupportedOperationException("There is currently no implementation for the given Datatype " + dataType);
        }
    }
}
