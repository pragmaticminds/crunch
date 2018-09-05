package org.pragmaticminds.crunch.runtime.cast;

import org.apache.flink.api.common.functions.MapFunction;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

/**
 * @author kerstin
 * Created by kerstin on 03.11.17.
 * This function casts UntypedValues to TypedValues.
 */
public class CastFunction implements MapFunction<UntypedValues, TypedValues> {

    /**
     * Casts an UntypedValues object to TypedValues object which is then returned
     *
     * @param untypedValues to be casted
     * @return the casted TypedValues
     * @throws Exception when a cast is not successful
     */
    @Override
    public TypedValues map(UntypedValues untypedValues) {
        return untypedValues.toTypedValues();
    }
}
