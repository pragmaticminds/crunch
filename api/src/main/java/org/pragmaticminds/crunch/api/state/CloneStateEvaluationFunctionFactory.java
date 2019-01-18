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

package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of the {@link EvaluationFunctionStateFactory}, which creates {@link EvaluationFunction} instances
 * on base of prototype original (as a blueprint) by cloning.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class CloneStateEvaluationFunctionFactory<T extends Serializable> implements EvaluationFunctionStateFactory<T> {
    private final EvaluationFunction<T> prototype;

    /**
     * private constructor for the builder
     * @param prototype of the EvaluationFunction to be cloned and used
     */
    private CloneStateEvaluationFunctionFactory(EvaluationFunction<T> prototype) {
        this.prototype = prototype;
    }

    /**
     * Clones the prototype object from instantiation
     *
     * @return a fresh cloned {@link EvaluationFunction} from the prototype
     */
    @Override
    public EvaluationFunction<T> create() {
        return ClonerUtil.clone(prototype);
    }

    /**
     * Collects all channel identifiers that are used in the inner {@link EvaluationFunction}.
     *
     * @return a {@link List} or {@link Collection} that contains all channel identifiers that are used.
     */
    @Override
    public Collection<String> getChannelIdentifiers() {
        return prototype.getChannelIdentifiers();
    }

    /**
     * creates a builder for this class
     * @return a builder
     */
    public static <T extends Serializable> Builder<T> builder() { return new Builder<>(); }

    /**
     * The builder for this class
     */
    public static final class Builder<T extends Serializable> {
        private EvaluationFunction<T> prototype;

        private Builder() { /* do nothing */ }

        public Builder<T> withPrototype(EvaluationFunction<T> prototype) {
            this.prototype = prototype;
            return this;
        }

        public Builder<T> but() {
            return CloneStateEvaluationFunctionFactory.<T>builder().withPrototype(prototype);
        }

        public CloneStateEvaluationFunctionFactory<T> build() {
            return new CloneStateEvaluationFunctionFactory<>(prototype);
        }
    }
}
