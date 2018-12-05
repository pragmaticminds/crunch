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

package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.trigger.comparator.SerializableAction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wraps the EvaluationFunction interface so that everything can be implemented as lambdas.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaEvaluationFunction implements EvaluationFunction {

    private SerializableAction<EvaluationContext> evalLambda;
    private SerializableResultFunction<HashSet<String>> getChannelIdentifiersLambda;

    /**
     * Main constructor that takes lambdas for the inner processing.
     *
     * @param evalLambda lambda that implements the base functionality of this {@link EvaluationFunction}.
     * @param getChannelIdentifiersLambda extracts the channel identifiers used by this {@link EvaluationFunction}.
     */
    public LambdaEvaluationFunction(
            SerializableAction<EvaluationContext> evalLambda,
            SerializableResultFunction<HashSet<String>> getChannelIdentifiersLambda
    ) {
        this.evalLambda = evalLambda;
        this.getChannelIdentifiersLambda = getChannelIdentifiersLambda;
    }

    /**
     * evaluates the incoming {@link TypedValues} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context
     *
     * @param ctx contains incoming data and a collector for the outgoing data
     */
    @Override
    public void eval(EvaluationContext ctx) {
        evalLambda.accept(ctx);
    }

    /**
     * Collects all channel identifiers, that are used for the triggering condition
     *
     * @return a {@link List} or {@link Collection} of all channel identifiers from triggering
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        return getChannelIdentifiersLambda.get();
    }
}
