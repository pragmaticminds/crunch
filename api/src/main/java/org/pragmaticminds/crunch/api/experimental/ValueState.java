package org.pragmaticminds.crunch.api.experimental;

import org.pragmaticminds.crunch.api.mql.DataType;

import java.util.Map;

/**
 * Represents a consistent state of all values.
 * <p>
 * Channel1: int
 * Channel2: long
 * Channel3: byte
 * <p>
 * => Evaluate(channel1:int, channel2:int)
 */
public class ValueState {

    private long timestamp; // Linux epoch time

    private Map<String, Object> values;
    private Map<String, DataType> dataTypes;


}
