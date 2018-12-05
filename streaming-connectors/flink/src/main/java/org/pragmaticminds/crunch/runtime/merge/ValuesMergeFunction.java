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

package org.pragmaticminds.crunch.runtime.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.execution.UntypedValuesMergeFunction;

import java.io.IOException;

/**
 * Merges all incoming {@link TypedValues} to one "common" Typed Values Object, the "State".
 * This state is then pushed downstream.
 * Wraps the {@link UntypedValuesMergeFunction} in a {@link MapFunction}.
 * @see UntypedValuesMergeFunction
 *
 * TM(2018.09.27) improved to Untyped (no cast nesseary) ... further improvement make it generic to {@link MRecord)
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValuesMergeFunction<T extends MRecord> extends RichMapFunction<T, MRecord> {
    private transient ValueState<UntypedValuesMergeFunction> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<UntypedValuesMergeFunction> descriptor = new ValueStateDescriptor<>(
                // state name
                "valueState-state",
                // type information of state
                TypeInformation.of(UntypedValuesMergeFunction.class)
        );
        valueState = getRuntimeContext().getState(descriptor);
    }

    /**
     * Delegates the call to {@link UntypedValuesMergeFunction#merge(MRecord)}  method.
     * @see UntypedValuesMergeFunction#merge(MRecord)
     * @param value to be merged
     * @return the merged value
     */
    @Override
    public MRecord map(T value) throws IOException {
        UntypedValuesMergeFunction mergeFunction = valueState.value();
        if(mergeFunction == null){
            mergeFunction = new UntypedValuesMergeFunction();
        }
        MRecord result = mergeFunction.merge(value);
        valueState.update(mergeFunction);
        return result;
    }
}
