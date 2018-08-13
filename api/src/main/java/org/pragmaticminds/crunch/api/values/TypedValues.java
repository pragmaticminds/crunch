package org.pragmaticminds.crunch.api.values;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.pragmaticminds.crunch.api.mql.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for Transporting "TypedValues" internally in CRUNCH.
 * Is the pendant to {@link UntypedValues} for the Untyped case.
 *
 * @author julian
 * Created by julian on 23.10.17
 */
@Data
@EqualsAndHashCode
@Builder
@NoArgsConstructor
public class TypedValues implements ValueEvent, Serializable {

    // Everything Transient, thus it uses custom serializers, see below.
    private String source;
    private long timestamp;
    private Map<String, Value> values;


    public TypedValues(String source, long timestamp, Map<String, Value> values) {
        this.source = source;
        this.timestamp = timestamp;
        // Add the values in a new hash map, otherwise one could insert an immutable map.
        this.values = new HashMap<>(values);
    }

    public double getDouble(String channel) {
        return values.get(channel).getAsDouble();
    }

    public long getLong(String channel) {
        return values.get(channel).getAsLong();
    }

    public boolean getBoolean(String channel) {
        return values.get(channel).getAsBoolean();
    }

    public Date getDate(String channel) {
        return values.get(channel).getAsDate();
    }

    public String getString(String channel) {
        if (!values.containsKey(channel)) {
            throw new InvalidParameterException("The requested Channel \"" + channel + "\" is not in the TypedValues Map!");
        }
        return values.get(channel).getAsString();
    }

    public Value get(String channel) {
        return values.get(channel);
    }

    /**
     * Merges another set of typed values into this typed values and returns the merged set.
     * This set is not updated!
     * <p>
     * It is important that the Values Object that is given to be merged into has higher (or equal) timestamp and
     * that both have the same source.
     *
     * @param typedValues Values to merge in
     * @return Merged Values
     */
    public TypedValues merge(TypedValues typedValues) {
        Preconditions.checkArgument(source.equals(typedValues.getSource()), "Both values have to come from the same source!");
        Preconditions.checkArgument(typedValues.getTimestamp() >= timestamp, "You try to merge a typed value that is older than the current state");

        Map<String, Value> newValues = new HashMap<>();
        newValues.putAll(values);
        newValues.putAll(typedValues.getValues());

        return new TypedValues(this.source,
                typedValues.getTimestamp(),
                newValues);
    }

    /**
     * Returns the requested value in the requested DataType (if possible).
     *
     * @param channel
     * @param dataType
     * @return
     * @throws UnsupportedOperationException if the cast cannot be done
     */
    public Object get(String channel, DataType dataType) {
        try {
            return this.values.get(channel).getAsDataType(dataType);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("Not able to return channel " + channel + " as " + dataType, e);
        }
    }
}
