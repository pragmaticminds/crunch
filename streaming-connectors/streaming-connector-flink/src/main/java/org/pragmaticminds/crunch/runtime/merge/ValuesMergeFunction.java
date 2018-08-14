package org.pragmaticminds.crunch.runtime.merge;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.pragmaticminds.crunch.api.values.TypedValues;

/**
 * Merges all incoming {@link TypedValues} to one "common" Typed Values Object, the "State".
 * This state is then pushed downstream.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValuesMergeFunction extends RichMapFunction<TypedValues, TypedValues> {

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
    public TypedValues map(TypedValues typedValues) throws Exception {
        // Fetch Flink State Object
        TypedValues values = valueState.value();
        // Do the mapping
        values = mapWithoutState(values, typedValues);
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
