package org.pragmaticminds.crunch.api.values;

import lombok.*;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class for Transporting "UntypedValues" e.g. over Kafka.
 *
 * @author julian
 * Created by julian on 23.10.17
 */
@Data
@EqualsAndHashCode
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UntypedValues implements ValueEvent {

    private String source;
    private long timestamp;
    private String prefix;
    private Map<String, Object> values;

    /**
     * converts UnTyped-Values to typed values
     * to differ between different sources (PLC-devices such as S71500, S7300, ...) a prefix is added to each Channel-Name for indication that those channels are not from main SPS
     *
     * @return regarding TypedValues
     */
    public TypedValues toTypedValues() {
        Map<String, Value> valueMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String channelName = entry.getKey();
            if (!prefix.isEmpty()) {
                channelName = prefix + "_" + entry.getKey();
            }
            valueMap.put(channelName, Value.of(entry.getValue()));
        }
        return new TypedValues(source, timestamp, valueMap);
    }

    /**
     * Returns a new {@link UntypedValues} Object which contains only values that are in the channels-set
     *
     * @param channels Set for the channels to keep
     * @return New {@link UntypedValues} which contains the "intersection" with the channels Set
     */
    public UntypedValues filterChannels(Set<String> channels) {
        Map<String, Object> filteredValues = values.entrySet().stream()
                .filter(entry -> channels.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new UntypedValues(source, timestamp, prefix, filteredValues);
    }

    /**
     * Returns true if there are no values in the Values-Map.
     *
     * @return
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    public String getPrefix() {
        return this.prefix;
    }

    public Map<String, Object> getValues() {
        return values;
    }
}
