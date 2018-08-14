package org.pragmaticminds.crunch.events;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.pragmaticminds.crunch.api.values.dates.Value;

import javax.management.openmbean.InvalidKeyException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an Event coming from a Machine, a Mixer or somewhere else.
 * Events are (effectively) immutable Data transfer Objects (DTO's) that can also be stored in the database
 * thus they are annotated with @{@link javax.persistence.Entity}.
 * They cannot be absolutely immutable due to JPA.
 * <p>
 * Events should only be created using a Builder {@link EventBuilder}.
 *
 * An Event consists of
 *  - timestamp (of the events beginning)
 *  - eventName (Category of the Event like "machine.cycle", "mixer.cycle", ...).
 *  - parameters (additional parameters to the value).
 *
 * Nested Parameter Values are given as "flattened" json like schema, if you have eg, a submap
 * <code>
 *     "code" -> 1
 *     "parameters.param1" -> 3.1
 *     "parameters.param2" -> 44123123
 * </code>
 *
 * @author julian
 * @author kerstin
 * Created by julian on 12.11.17
 */
@ToString
@NoArgsConstructor
@EqualsAndHashCode
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Event implements Serializable {

    private Long timestamp;

    private String eventName;

    /**
     * Name of the events source.
     */
    private String eventSource;

    private HashMap<String, Value> parameters;


    public Event(long timestamp, String eventName, String source, Map<String, Value> parameters) {
        this.timestamp = timestamp;
        this.eventName = eventName;
        this.eventSource = source;
        this.parameters = parameters == null ? new HashMap<>() : new HashMap<>(parameters);
    }

    public Event(long timestamp, String eventName, String source) {
        this.timestamp = timestamp;
        this.eventName = eventName;
        this.eventSource = source;
        this.parameters = new HashMap<>();
    }

    public Event(Event other) {
        this.timestamp = other.timestamp;
        this.eventName = other.eventName;
        this.eventSource = other.eventSource;
        this.parameters = other.parameters;
    }

    public Event(UntypedEvent other) {
        this.eventName = other.getEventName();
        this.eventSource = other.getEventSource();
        this.timestamp = other.getTimestamp();
        this.parameters = new HashMap<>();
        other.getParameters().forEach((key, value)->
                this.parameters.put(key, Value.of(value))
        );
    }

    public Value getParameter(String parameter) {
        if (!parameters.containsKey(parameter)) {
            throw new InvalidKeyException("No parameter with name \"" + parameter + "\" present in the Event.");
        }
        return parameters.get(parameter);
    }

    /**
     * Setter used to set the soruce in the (already generated) event.
     *
     * @param eventSource source name.
     */
    public void setEventSource(String eventSource) {
        this.eventSource = eventSource;
    }

    public String getSource() {
        return this.eventSource;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public Map<String, Value> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Value> parameters) {
        this.parameters = parameters == null ? new HashMap<>() : new HashMap<>(parameters);
    }

}
