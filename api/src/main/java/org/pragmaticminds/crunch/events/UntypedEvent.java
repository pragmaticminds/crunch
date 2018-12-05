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

package org.pragmaticminds.crunch.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.pragmaticminds.crunch.api.values.dates.DateValue;
import org.pragmaticminds.crunch.api.values.dates.Value;

import javax.management.openmbean.InvalidKeyException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/**
 * Same as {@link GenericEvent}, but {@link Object} instead of {@link Value}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 17.11.2017
 */
@ToString
@EqualsAndHashCode
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("squid:S1319") // HashMap is needed to make it serializable, Map isn't.
public class UntypedEvent implements Event, Serializable {

    private String eventName;
    private String eventSource;
    private HashMap<String, Object> parameters;
    private Long timestamp;

    public UntypedEvent() { /* for JPA */ }

    public UntypedEvent(GenericEvent other) {
        this.eventName = other.getEventName();
        this.eventSource = other.getSource();
        this.timestamp = other.getTimestamp();
        this.parameters = new HashMap<>();
        other.getParameters().forEach((key, value) -> {
            if (value instanceof DateValue) {
                this.parameters.put(key, new SerializableDate(value.getAsDate()));
            } else {
                this.parameters.put(key, value.getAsObject());
            }
        });
    }

    /**
     * conversion from {@link GenericEvent} to {@link UntypedEvent}
     *
     * @param event to be converted to {@link UntypedEvent}
     * @return the result of the conversion
     */
    public static UntypedEvent fromEvent(GenericEvent event) {
        return new UntypedEvent(event);
    }

    public Object getParameter(String parameter) {
        if (!parameters.containsKey(parameter)) {
            throw new InvalidKeyException(String.format(
                    "No parameter with name \"%s\" present in the GenericEvent.",
                    parameter
            ));
        }
        return parameters.get(parameter);
    }

    /**
     * converts this object to am {@link GenericEvent}
     *
     * @return result of the conversion
     */
    public GenericEvent asEvent() {
        return new GenericEvent(this);
    }

    /**
     * Helper class for serialization and deserialization
     */
    @EqualsAndHashCode
    @ToString
    @Getter
    public static class SerializableDate {
        private long date;

        public SerializableDate(Date date) {
            this.date = date.toInstant().toEpochMilli();
        }

        public Date asDate() {
            return Date.from(Instant.ofEpochMilli(date));
        }
    }
}
