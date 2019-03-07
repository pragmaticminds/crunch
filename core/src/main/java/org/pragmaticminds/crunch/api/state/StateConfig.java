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

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuring Object for one State in the {@link MultiStepEvaluationFunction}.
 * Contains all necessary information as DTO.
 *
 * @author julian
 * Created by julian on 05.09.18
 */
public class StateConfig<T extends Serializable> implements Serializable {

    private final String stateAlias;
    private final EvaluationFunctionStateFactory<T> factory;
    private final long stateTimeout;

    public StateConfig(String stateAlias, EvaluationFunctionStateFactory<T> factory, long stateTimeout) {
        this.stateAlias = stateAlias;
        this.factory = factory;
        this.stateTimeout = stateTimeout;
    }

    /**
     * Create a new Instance of the Evaluation Function for this state.
     *
     * @return New Instance
     */
    public EvaluationFunction<T> create() {
        return getFactory().create();
    }

    public String getStateAlias() {
        return stateAlias;
    }

    public EvaluationFunctionStateFactory<T> getFactory() {
        return factory;
    }

    public long getStateTimeout() {
        return stateTimeout;
    }

    @Override
    public String toString() {
        return "StateConfig{" +
                "stateAlias='" + stateAlias + '\'' +
                ", factory=" + factory +
                ", stateTimeout=" + stateTimeout +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        StateConfig that = (StateConfig) o;
        return getStateTimeout() == that.getStateTimeout() &&
                Objects.equals(getStateAlias(), that.getStateAlias()) &&
                Objects.equals(getFactory(), that.getFactory());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStateAlias(), getFactory(), getStateTimeout());
    }
}
