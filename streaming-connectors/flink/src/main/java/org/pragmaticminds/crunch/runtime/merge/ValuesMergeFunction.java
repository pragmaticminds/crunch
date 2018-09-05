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
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValuesMergeFunction extends RichMapFunction<MRecord, MRecord> {

    private transient ValueState<TypedValues> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Get Value State
        ValueStateDescriptor<TypedValues> descriptor = new ValueStateDescriptor<>(
                // state name
                "valueState-state",
                // type information of state
                TypeInformation.of(TypedValues.class));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public MRecord map(MRecord value) throws Exception {
        // Merge the Records
        TypedValues currentValue;
        if (TypedValues.class.isInstance(value)) {
            currentValue = (TypedValues) value;
        } else if (UntypedValues.class.isInstance(value)) {
            currentValue = ((UntypedValues) value).toTypedValues();
        } else {
            throw new InvalidParameterException("ValuesMergeFunction currently only supports TypedValues and " +
                    "UntypedValues and not " + value.getClass().getName());
        }
        // Fetch Flink State Object
        TypedValues values = valueState.value();
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
    TypedValues mapWithoutState(TypedValues currentValues, TypedValues newValues) {
        // Init the valueState object on first value object
        TypedValues state;
        if (currentValues == null) {
            state = newValues;
        } else {
            state = currentValues.merge(newValues);
        }
        return state;
    }

}
