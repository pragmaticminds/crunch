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

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Consumer;

public class Evaluate<IN, EVENT extends Serializable> extends AbstractStreamNode<IN, EVENT> {

  private final EvaluationFunction<EVENT> evaluation;

  public Evaluate(EvaluationFunction<EVENT> evaluation) {
    this.evaluation = evaluation;
  }

  public EvaluationFunction<EVENT> getEvaluation() {
    return evaluation;
  }

  @Override public <T> T accept(StreamNodeVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public ResultHandler<EVENT> handle(Consumer<EVENT> consumer) {
    final ResultHandler<EVENT> handler = new ResultHandler<>(consumer);
    this.addChild(handler);
    return handler;
  }

  public ResultHandler<EVENT> handle(Object instance, String method, Class<EVENT> clazz) {
    // Resolve method on class
    try {
      final Method m = instance.getClass().getMethod(method, clazz);
      return this.handle(e -> {
        try {
          m.invoke(instance, e);
        } catch (IllegalAccessException | InvocationTargetException e1) {
          throw new RuntimeException("Unnable to invoce the caller method");
        }
      });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("No such method found in instance object");
    }
  }
}
