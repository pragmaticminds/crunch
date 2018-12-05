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

package org.pragmaticminds.crunch;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.*;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.execution.EventSink;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Tests for the feature CRUNCH-577 that PoJos can be used as Events.
 * This is just an Example on how to use this feature.
 *
 * @author julian
 * Created by julian on 02.10.18
 */
public class PojoEventTest implements Serializable {

    @Test
    public void usePojoAsEventCrunchExecutor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EvaluationPipeline<MyPojo> pipeline = createPipeline();

        CrunchFlinkPipelineFactory<MyPojo> factory = new CrunchFlinkPipelineFactory<>();
        MRecord values = UntypedValues.builder()
                .source("")
                .timestamp(1L)
                .prefix("")
                .values(Collections.singletonMap("key", 1L))
                .build();
        factory.create(env.fromCollection(Arrays.asList(values)), pipeline, MyPojo.class)
                .print();

        env.execute();
    }

    private EvaluationPipeline<MyPojo> createPipeline() {
        EvaluationFunction<MyPojo> function = new EvaluationFunction<MyPojo>() {
            @Override
            public void eval(EvaluationContext<MyPojo> ctx) {
                ctx.collect(new MyPojo("Hello from the eval function"));
            }

            /**
             * Returns all channel identifiers which are necessary for the function to do its job.
             * It is not allowed to return null, an empty set can be returned (but why should??).
             *
             * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
             */
            @Override
            public Set<String> getChannelIdentifiers() {
                return Collections.singleton("key");
            }
        };

        SubStream<MyPojo> stream = SubStream.<MyPojo>builder()
                .withIdentifier("id")
                .withPredicate((SubStreamPredicate) values -> true)
                .withEvaluationFunction(function)
                .build();

        return EvaluationPipeline.<MyPojo>builder()
                .withIdentifier("my pipeline")
                .withSubStream(stream)
                .build();
    }

    private static class MyPojo implements Serializable {

        private final String value;

        public MyPojo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "MyPojo{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    private static class PojoEventSink implements EventSink<MyPojo> {

        @Override
        public void handle(MyPojo event) {
            System.out.println("Value of the Pojo: " + event.getValue());
        }
    }
}
