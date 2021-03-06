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

import com.google.common.base.Preconditions;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;

/**
 * This implementation of the EvaluationContext binds to a Flink {@link Collector}, so all collected {@link GenericEvent}s are
 * automatically in the output stream of Flink.
 * This class is created before a processing of {@link EvaluationFunction}s can be started.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class CollectorEvaluationContext<T extends Serializable> extends EvaluationContext<T> {
    private final MRecord value;
    private final transient Collector<T> out;

    /**
     * private constructor for the inner {@link Builder} class.
     * @param value the current one to be processed
     * @param out the outgoing result {@link Collector} of Flink
     */
    private CollectorEvaluationContext(
            MRecord value, Collector<T> out) {
        this.value = value;
        this.out = out;
    }

    /**
     * delivers the next {@link MRecord} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public MRecord get() {
        return this.value;
    }

    /**
     * collects the resulting Ts of processing
     *
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    @Override
    public void collect(T event) {
        out.collect(event);
    }

    /**
     * Creates a builder for this class {@link CollectorEvaluationContext}
     * @return a builder for this class {@link CollectorEvaluationContext}
     */
    public static <T extends Serializable> Builder<T> builder() { return new Builder<>(); }

    /**
     * Creates instances of the type {@link CollectorEvaluationContext}
     */
    public static final class Builder<T extends Serializable> {
        private MRecord value;
        private Collector<T> out;

        private Builder() {}

        public Builder withValue(MRecord value) {
            this.value = value;
            return this;
        }

        public Builder withOut(Collector<T> out) {
            this.out = out;
            return this;
        }

        @SuppressWarnings("unchecked") // manually checked
        public Builder but() { return builder().withValue(value).withOut(out); }

        public CollectorEvaluationContext build() {
            checkParameters();
            return new CollectorEvaluationContext<T>(value, out);
        }

        private void checkParameters() {
            Preconditions.checkNotNull(value);
            Preconditions.checkNotNull(out);
        }
    }
}
