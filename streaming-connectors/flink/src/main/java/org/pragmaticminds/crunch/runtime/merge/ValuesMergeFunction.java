package org.pragmaticminds.crunch.runtime.merge;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.security.InvalidParameterException;

/**
 * Merges all incoming {@link TypedValues} to one "common" Typed Values Object, the "State".
 * This state is then pushed downstream.
 *
 * TODO Implement a more efficient {@link ValuesMergeFunction#map(MRecord)} version (as this always casts to typed!!!)
 * TM(2018.09.27) improved to Untyped (no cast nesseary) ... further improvement make it generic to {@link MRecord)
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValuesMergeFunction extends RichMapFunction<MRecord, MRecord> {

    private transient ValueState<UntypedValues> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Get Value State
        ValueStateDescriptor<UntypedValues> descriptor = new ValueStateDescriptor<>(
                // state name
                "valueState-state",
                // type information of state
                TypeInformation.of(UntypedValues.class));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public MRecord map(MRecord value) throws Exception {
        // Merge the Records
        UntypedValues currentValue;
        if (UntypedValues.class.isInstance(value)) {
            currentValue = (UntypedValues) value;
        } else {
            throw new InvalidParameterException("ValuesMergeFunction currently only supports " +
                    "UntypedValues and not " + value.getClass().getName());
        }
        // Fetch Flink State Object
        UntypedValues values = valueState.value();
        // Do the mapping
        values = mapWithoutState(values, currentValue);
        // Return Flink State Object
        valueState.update(values);
        return values;
    }

    /**
     * Internal method that does the merging of the state.
     * Does not fetch / rewrite the Function's state.
     *
     * @param currentValues
     * @param newValues
     * @return
     */
    UntypedValues mapWithoutState(UntypedValues currentValues, UntypedValues newValues) {
        // Init the valueState object on first value object
        UntypedValues state;
        if (currentValues == null) {
            state = newValues;
        } else {
            state = currentValues.merge(newValues);
        }
        return state;
    }

}
