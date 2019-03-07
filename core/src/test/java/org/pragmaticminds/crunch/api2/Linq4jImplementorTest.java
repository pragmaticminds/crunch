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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Linq4jImplementorTest {

//  @Test
//  public void implement() throws NoSuchMethodException {
//    final CrunchContext context = new CrunchContext();
//    final Root<Integer> root = new Root<>(context, Linq4j.asEnumerable(Arrays.asList(1, 2, 3, 4)));
//    final Filter<Integer> filter = new Filter<>(i -> (i % 5) == 0);
//    root.addChild(filter);
//    final GroupBy<Integer, Integer> groupBy = new GroupBy<>(i -> i % 5);
//    filter.addChild(groupBy);
//    final Evaluate<Integer, Serializable> evaluate = new Evaluate<>(null);
//    groupBy.addChild(evaluate);
//    final ResultHandler<Serializable> handler = new ResultHandler<>(null, Linq4jImplementorTest.class.getMethod("print", Object.class));
//    evaluate.addChild(handler);
//
//    final Linq4jImplementor implementor = new Linq4jImplementor(root.getValues(), filter.predicate, groupBy.groupAssigner, Collections.singletonList(new Linq4jImplementor.EvaluationANdHandler<>(evaluate.getEvaluation(), handler.getConsumer())));
//
//    final Enumerator enumerator = implementor.implement();
//
//    while (enumerator.moveNext()) {
//      enumerator.current();
//    }
//
//  }

  public static void print(Object o) {
    System.out.println(o);
  }

  @Test
  public void implement2() throws NoSuchMethodException {
    final CrunchContext context = new CrunchContext();
    final Root<UntypedValues> root = new Root<>(context, new AbstractEnumerable<UntypedValues>() {
      @Override public Enumerator<UntypedValues> enumerator() {
        return new Enumerator<UntypedValues>() {

          private Supplier<UntypedValues> source = getSignal();
          private UntypedValues current = null;
          private long count = 0;

          @Override public UntypedValues current() {
            return current;
          }

          @Override public boolean moveNext() {
            this.current = source.get();
            return count++ < 100;
          }

          @Override public void reset() {

          }

          @Override public void close() {

          }
        };
      }
    });
    final Filter<UntypedValues> filter = new Filter<>(i -> true);
    root.addChild(filter);
    final GroupBy<UntypedValues, String> groupBy = new GroupBy<>(u -> u.getSource());
    filter.addChild(groupBy);
    final Evaluate<UntypedValues, Serializable> evaluate = new Evaluate<>(new SerializableEvaluationFunction());
    groupBy.addChild(evaluate);
    final ResultHandler<Serializable> handler = new ResultHandler<>(null, Linq4jImplementorTest.class.getMethod("print", Object.class));
    evaluate.addChild(handler);

    final Linq4jImplementor<UntypedValues, GenericEvent> implementor = new Linq4jImplementor<>(root);
    final Enumerator enumerator = implementor.implement();

    while (enumerator.moveNext()) {
      enumerator.current();
    }
  }

  private Supplier<UntypedValues> getSignal() {
    return new Supplier<UntypedValues>() {

      AtomicInteger counter = new AtomicInteger(0);

      @Override public UntypedValues get() {
        final int val = counter.getAndIncrement();
        return new UntypedValues("source_" + (val % 3), val, "", Collections.singletonMap("signal", val));
      }
    };
  }

  private static class SerializableEvaluationFunction implements EvaluationFunction<Serializable> {

    private int counter = 0;

    @Override public void eval(EvaluationContext<Serializable> ctx) {
      ctx.collect(counter++);
    }

    @Override public Set<String> getChannelIdentifiers() {
      return Collections.emptySet();
    }
  }
}