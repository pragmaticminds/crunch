package org.pragmaticminds.crunch.api.values;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for Transporting "TypedValues" internally in CRUNCH.
 * Is the pendant to {@link UntypedValues} for the Untyped case.
 *
 * It is one implementation of {@link MRecord} but as it holds all values typed in the {@link Value} classes (and inherited) classes
 * it is very expensive to create and usually only a small subset of the Values is really requested.
 *
 * @author julian
 * Created by julian on 23.10.17
 */
@Data
@EqualsAndHashCode
@Builder
@NoArgsConstructor
public class TypedValues implements MRecord, Serializable {

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

    @Override
    public double getDouble(String channel) {
        return values.get(channel).getAsDouble();
    }

    @Override
    public long getLong(String channel) {
        return values.get(channel).getAsLong();
    }

    @Override
    public boolean getBoolean(String channel) {
        return values.get(channel).getAsBoolean();
    }

    @Override
    public Date getDate(String channel) {
        return values.get(channel).getAsDate();
    }

    @Override
    public String getString(String channel) {
        if (!values.containsKey(channel)) {
            throw new InvalidParameterException("The requested Channel \"" + channel + "\" is not in the TypedValues Map!");
        }
        return values.get(channel).getAsString();
    }

    @Override
    public Value getValue(String channel) {
        return values.get(channel);
    }

    @Override
    public Object get(String channel) {
        throw new UnsupportedOperationException("This is a typed object untyped getter not supported!");
    }

    @Override
    public Collection<String> getChannels() {
        return this.values.keySet();
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

}
