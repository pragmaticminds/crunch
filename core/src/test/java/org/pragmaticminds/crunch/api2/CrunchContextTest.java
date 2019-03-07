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

package org.pragmaticminds.crunch.api2;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public class CrunchContextTest {

  @Test
  public void createStream() {
    final CrunchContext context = new CrunchContext();

    // Message builder to build an un. val. with a given source
    Function<String, UntypedValues> builder = t -> {
      return UntypedValues.builder()
          .source(t)
          .prefix("")
          .timestamp(0L)
          .values(Collections.singletonMap("temp", 24.5))
          .build();
    };

    // Create a stream
    final GroupBy<UntypedValues, String> groupedStream = context
        .values(builder.apply("source1"),
            builder.apply("source1"), builder.apply("source2"))
        .filter(ut -> ut.getTimestamp() <= 2)
        .groupBy(UntypedValues::getSource);

    // First evaluation function
    groupedStream
        .evaluate(evaluation("eval1"))
        .handle(System.out::println);

    // Second evaluation Function
    groupedStream
        .evaluate(evaluation("eval2"))
        .handle(System.out::println);

    // Evaluation Function with custom Event
    groupedStream
        .evaluate(new PojoEvaluationFunction())
        .handle(this, "asdf", MyPOJO.class);

    context.execute();
  }

  public void asdf(MyPOJO pojo) {
    System.out.println("This is the POJO " + pojo);
  }

  private EvaluationFunction<GenericEvent> evaluation(String eval2) {
    return new GenericEventEvaluationFunction(eval2);
  }

  private static class GenericEventEvaluationFunction implements EvaluationFunction<GenericEvent> {

    private final String eval2;
    private int count = 0;

    public GenericEventEvaluationFunction(String eval2) {
      this.eval2 = eval2;
    }

    @Override public void eval(EvaluationContext<GenericEvent> ctx) {
      ctx.collect(new GenericEvent(count++, eval2, ctx.get().getSource()));
    }

    @Override public Set<String> getChannelIdentifiers() {
      return null;
    }
  }

  public static class PojoEvaluationFunction implements EvaluationFunction<MyPOJO> {

    @Override public void eval(EvaluationContext<MyPOJO> ctx) {
      ctx.collect(new MyPOJO("Received: " + ctx.get().toString()));
    }

    @Override public Set<String> getChannelIdentifiers() {
      return null;
    }
  }

  public static class MyPOJO implements Serializable {

    private final String a;

    public MyPOJO(String a) {
      this.a = a;
    }

    @Override public String toString() {
      return "MyPOJO{" +
          "a='" + a + '\'' +
          '}';
    }
  }
}