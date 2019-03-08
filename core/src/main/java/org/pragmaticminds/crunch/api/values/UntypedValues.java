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

package org.pragmaticminds.crunch.api.values;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class for Transporting "UntypedValues" e.g. over Kafka.
 * If one has {@link MRecord}s with many values and only a small subset is frequently used it is way more efficint
 * to use this class than {@link TypedValues}.
 *
 * @see TypedValues
 *
 * @author julian
 * @author Erwin Wagasow
 * Created by julian on 23.10.17
 * Modified by Erwin Wagasow on 06.09.2018
 */
@Data
@EqualsAndHashCode
@NoArgsConstructor
public class UntypedValues implements MRecord {
    private static final Logger logger = LoggerFactory.getLogger(UntypedValues.class);

    private String source;
    private long timestamp;
    private String prefix;
    private HashMap<String, Object> values;

    public UntypedValues(
            String source, long timestamp, String prefix, Map<String, Object> values
    ) {
        this.source = source;
        this.timestamp = timestamp;
        this.prefix = prefix;
        this.values = values == null ? null : new HashMap<>(values);
    }

    @Override
    public Double getDouble(String channel) {
        Value v = getValue(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return v.getAsDouble();
    }
    
    @Override
    public Long getLong(String channel) {
        Value v = getValue(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return v.getAsLong();
    }

    @Override
    @SuppressWarnings("squid:S2447") // null should not be returned, in this case it is necessary
    public Boolean getBoolean(String channel) {
        Value v = getValue(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return v.getAsBoolean();
    }

    @Override
    public Date getDate(String channel) {
        Value v = getValue(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return v.getAsDate();
    }

    @Override
    public String getString(String channel) {
        Value v = getValue(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return v.getAsString();
    }

    @Override
    public Value getValue(String channel) {
        Object v = get(channel);
        if (v == null) {
            printError(channel);
            return null;
        }
        return Value.of(v);
    }

    @Override
    public Object get(String channel) {
        if (!values.containsKey(channel)) {
            printError(channel);
            return null;
        }
        return values.get(channel);
    }

    @Override
    @JsonIgnore
    public Collection<String> getChannels() {
        return this.values.keySet();
    }
    
    private void printError(String channel) {
        logger.error("Channel with the name \"{}\" is not present!", channel);
    }

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
            if (prefix != null && !prefix.isEmpty()) {
                channelName = prefix + "_" + entry.getKey();
            }
            valueMap.put(channelName, Value.of(entry.getValue()));
        }
        return new TypedValues(source, timestamp, valueMap);
    }

    /**
     * Returns a new {@link UntypedValues} Object which contains only values that are in the channels-set
     * @param channels Set for the channels to keep
     * @return New {@link UntypedValues} which contains the "intersection" with the channels Set
     */
    public UntypedValues filterChannels(Set<String> channels) {
        Map<String, Object> filteredValues = values.entrySet().stream()
                .filter(entry -> channels.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new UntypedValues(source, timestamp, prefix, new HashMap<>(filteredValues));
    }

    /**
     * Returns true if there are no values in the Values-Map.
     * @return
     */
    @JsonIgnore // prevent interpretation as a getter
    public boolean isEmpty() {
        return values.isEmpty();
    }

    public String getPrefix() {
        return this.prefix;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    /**
     * setter makes {@link HashMap} from {@link Map}
     * @param values to be saved as {@link HashMap}
     */
    public void setValues(Map<String, Object> values) {
        this.values = values == null ? null : new HashMap<>(values);
    }

    /**
     * Merges another set of untyped values into this untyped values and returns the merged set.
     * This set is not updated!
     *
     * @param untypedValues Values to merge in
     * @return Merged Untyped Values
     */
    public UntypedValues merge(UntypedValues untypedValues) {
        Preconditions.checkArgument(this.source.equals(untypedValues.getSource()), "Both values have to come from the same source!");
        Preconditions.checkArgument(untypedValues.getTimestamp() >= timestamp, "You try to merge a untyped value that is older than the current state.");
        Preconditions.checkArgument(this.prefix.equals(untypedValues.getPrefix()), "Both prefixes must be the same.");

        // Test whether this is faster (or how much faster)
        this.values.putAll(untypedValues.getValues());
        return this;
        /**
         Map<String, Object> newValues = new HashMap<>();
         newValues.putAll(values);
         newValues.putAll(untypedValues.getValues());

         return new UntypedValues(this.source,
         untypedValues.getTimestamp(),
         this.prefix,
         newValues);
         */
    }

    /**
     * Creates a builder for this class
     * @return a builder for this class
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder for this class
     */
    public static final class Builder {
        private String              source;
        private long                timestamp;
        private String              prefix;
        private Map<String, Object> values;

        private Builder() {}


        public Builder source(String source) {
            this.source = source;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder values(Map<String, Object> values) {
            this.values = values;
            return this;
        }

        public Builder but() {
            return builder().source(source)
                    .timestamp(timestamp)
                    .prefix(prefix)
                    .values(values);
        }

        public UntypedValues build() {
            UntypedValues untypedValues = new UntypedValues();
            untypedValues.setSource(source);
            untypedValues.setTimestamp(timestamp);
            untypedValues.setPrefix(prefix);
            untypedValues.setValues(values);
            return untypedValues;
        }
    }
}
