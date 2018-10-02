package org.pragmaticminds.crunch.events;


import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder for an {@link GenericEvent}.
 * Should always be used for creation.
 *
 * @author julian
 * Created by julian on 12.11.17
 */
public final class GenericEventBuilder implements EventBuilder<GenericEvent> {
    private Long timestamp;
    private String event;

    /**
     * Default value is the unknown source value.
     */
    private String source;

    private Map<String, Value> parameters;

    private GenericEventBuilder() {
    }

    public static GenericEventBuilder anEvent() {
        return new GenericEventBuilder();
    }

    public GenericEventBuilder withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public GenericEventBuilder withEvent(String event) {
        this.event = event;
        return this;
    }

    public GenericEventBuilder withParameters(Map<String, Value> parameters) {
        this.parameters = parameters;
        return this;
    }

    public GenericEventBuilder withParameter(String parameter, Value value) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, value);
        return this;
    }

    /**
     * @param parameter as String
     * @param l         as Long
     * @return an event
     */
    public GenericEventBuilder withParameter(String parameter, Long l) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, Value.of(l));
        return this;
    }

    /**
     * @param parameter as String
     * @param d         as Double
     * @return an event
     */
    public GenericEventBuilder withParameter(String parameter, Double d) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, Value.of(d));
        return this;
    }

    /**
     * @param parameter as String
     * @param s         as String
     * @return an event
     */
    public GenericEventBuilder withParameter(String parameter, String s) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, Value.of(s));
        return this;
    }

    /**
     * @param parameter as String
     * @param date      as Date
     * @return an event
     */
    public GenericEventBuilder withParameter(String parameter, Date date) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, Value.of(date));
        return this;
    }

    /**
     * @param parameter as String
     * @param b         as Boolean
     * @return an event
     */
    public GenericEventBuilder withParameter(String parameter, Boolean b) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(parameter, Value.of(b));
        return this;
    }

    public GenericEventBuilder withSource(String source) {
        this.source = source;
        return this;
    }
    
    /**
     * Creates a new instance of the {@link GenericEvent} class.
     *
     * @return new {@link GenericEvent} instance.
     */
    @Override
    public GenericEvent build() {
        Preconditions.checkNotNull(timestamp, "Specify timestamp");
        Preconditions.checkNotNull(event, "Specify event type");
        Preconditions.checkNotNull(source, "Specify source");
        return new GenericEvent(timestamp, event, source, parameters);
    }
}
