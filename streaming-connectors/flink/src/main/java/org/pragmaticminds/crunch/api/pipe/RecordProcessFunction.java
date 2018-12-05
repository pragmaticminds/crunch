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
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.util.List;

/**
 * Creates a {@link ProcessFunction} binded to a list of {@link RecordHandler}s from a SubStream.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class RecordProcessFunction extends ProcessFunction<MRecord, Void> {

    private List<RecordHandler> recordHandlers;

    /**
     * private constructor for the {@link Builder}
     * @param recordHandlers a list of {@link RecordHandler}s, that are to be integrated into
     *                       the processing of this class.
     */
    private RecordProcessFunction(List<RecordHandler> recordHandlers) {
        this.recordHandlers = recordHandlers;
    }

    /**
     * Creates a new instance of the {@link Builder}
     * @return a new instance of the {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter
     * and also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting
     *              a {@link TimerService} for registering timers and querying the time. The
     *              context is only valid during the invocation of this method, do not store it.
     * @param out   The collector for returning result values.
     */
    @Override
    public void processElement(MRecord value, Context ctx, Collector<Void> out) {
        // process the incoming values with the cloned evaluation functions
        for (RecordHandler recordHandler : this.recordHandlers) {
            recordHandler.apply(value);
        }
    }

    /**
     * Creates instances of the {@link RecordProcessFunction} and checks ingoing parameters
     */
    public static final class Builder {
        private List<RecordHandler> recordHandlers;

        private Builder() {}

        public Builder withRecordHandlers(List<RecordHandler> recordHandlers) {
            this.recordHandlers = recordHandlers;
            return this;
        }

        public Builder but() { return builder().withRecordHandlers(recordHandlers); }

        public RecordProcessFunction build() {
            checkParameter(recordHandlers);
            return new RecordProcessFunction(recordHandlers);
        }

        private void checkParameter(List<RecordHandler> recordHandlers) {
            Preconditions.checkNotNull(recordHandlers);
            Preconditions.checkArgument(!recordHandlers.isEmpty());
        }
    }
}
