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

package org.pragmaticminds.crunch.api;

import org.pragmaticminds.crunch.api.annotations.AnnotationUtils;
import org.pragmaticminds.crunch.api.annotations.ChannelValue;
import org.pragmaticminds.crunch.api.annotations.ParameterValue;
import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.records.DataType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is used to test {@link EvalFunction} classes for their functionality
 * <p>
 * Created by Erwin Wagasow on 22.06.2017.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class AnnotatedEvalFunctionTestTool {

    private List<Object> parameterValueHolder = new ArrayList<>();
    private List<Object> channelAndFunctionHolders = new ArrayList<>();

    private EvalFunctionTestTool.EvaluationTestToolEvents events;
    private AnnotatedEvalFunction annotatedEvalFunction;

    /**
     * Creates a testing surrounding for {@link AnnotatedEvalFunction} classes
     *
     * @param annotatedEvalFunctionClass {@link Class} of a {@link AnnotatedEvalFunction}, that should be tested
     * @throws IllegalAccessException should not happen, if happened, than some classes are not reachable from current
     *                                context
     * @throws InstantiationException can be thrown
     */
    public AnnotatedEvalFunctionTestTool(Class<? extends AnnotatedEvalFunction> annotatedEvalFunctionClass) throws IllegalAccessException,
            InstantiationException {
        @SuppressWarnings("squid:S1612") //cannot make this a lambda
                GenericEventHandler eventHandler = event -> events.addEvent(event);

        // create instance of the evaluation class
        annotatedEvalFunction = annotatedEvalFunctionClass.newInstance();

        // find and set all parameter annotations and set holders
        List<Field> parameterFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunction, ParameterValue.class);
        parameterFields.forEach(parameter -> {
            DataType dataType = parameter.getAnnotation(ParameterValue.class).dataType();
            Holder inParameterHolder = new Holder(null, dataType.getClassType());
            parameterValueHolder.add(inParameterHolder);
        });
        AnnotationUtils.setValuesToAnnotatedType(parameterValueHolder, annotatedEvalFunction, ParameterValue.class);

        // find and set all channelValue annotations and set holders
        List<Field> channelAndFunctionFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunction, ChannelValue.class);
        channelAndFunctionFields.forEach(channelOrFunction -> {
            DataType dataType = channelOrFunction.getAnnotation(ChannelValue.class).dataType();
            Holder inChannelAndFunctionHolder = new Holder(null, dataType.getClassType());
            channelAndFunctionHolders.add(inChannelAndFunctionHolder);
        });
        AnnotationUtils.setValuesToAnnotatedType(channelAndFunctionHolders, annotatedEvalFunction, ChannelValue.class);

        // inject result handler into the function
        AnnotationUtils.injectEventStream(annotatedEvalFunction, eventHandler);
    }

    /**
     * executes a test run of the EvaluationFunction class implementation with the given test data
     *
     * @param inParameters All literals, which are used in the EvaluationFunction in the order of their definition in the class
     * @param inChannels   A Table of test data, by first index is row, second is column
     * @return the Results object containing all data that either was put out as output or as result event
     */
    public EvalFunctionTestTool.EvaluationTestToolEvents execute(List<Object> inParameters, List<List<Object>> inChannels) {

        events = new EvalFunctionTestTool.EvaluationTestToolEvents();

        // set the in parameter values into the holders
        AtomicReference<Integer> index = new AtomicReference<>(0);
        inParameters.forEach(inParameter -> {
            ((Holder) this.parameterValueHolder.get(index.get())).set(inParameter);
            index.set(index.get() + 1);
        });

        // Phase 1
        annotatedEvalFunction.setup();

        // Phase 2
        // set the values to the holders
        inChannels.forEach(inRows -> {
            index.set(0);
            inRows.forEach(inValue -> {
                ((Holder) this.channelAndFunctionHolders.get(index.get())).set(inValue);
                index.set(index.get() + 1);
            });
            Object output = annotatedEvalFunction.eval();
            this.events.addOutput(output);
        });

        // Phase 3
        annotatedEvalFunction.finish();

        return events;
    }

}
