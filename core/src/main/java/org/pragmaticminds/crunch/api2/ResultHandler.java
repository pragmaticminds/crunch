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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Final "closing" block of a Crunch Stream.
 * It contains the logic how the Events should be "handled".
 * For that it contains a consumer or a crunch sink.
 *
 * @param <EVENT> Type of events.
 */
public class ResultHandler<EVENT extends Serializable> implements StreamNode<EVENT, Void> {

  private final Object instance;
  private final Method method;
  private final Consumer<EVENT> consumer;

  public ResultHandler(Object instance, Method method) {
    this(instance, method, null);
  }

  public ResultHandler(Consumer<EVENT> consumer) {
    this(null, null, consumer);
  }

  public ResultHandler(Object instance, Method method, Consumer<EVENT> consumer) {
    this.instance = instance;
    this.method = method;
    this.consumer = consumer;
  }

  public Object getInstance() {
    return instance;
  }

  public Method getMethod() {
    return method;
  }

  /** Return either the given one or create one */
  public Consumer<EVENT> getConsumer() {
    if (consumer != null) {
      return consumer;
    }
    return new Consumer<EVENT>() {
      @Override public void accept(EVENT event) {
        try {
          method.invoke(instance, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
      }
    };
  }

  @Override public <T> T accept(StreamNodeVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override public void addChild(StreamNode<Void, ?> child) {
    throw new UnsupportedOperationException("Result Handler is a final node and has no children");
  }

  @Override public List<StreamNode<Void, ?>> getChildren() {
    return Collections.emptyList();
  }

  // TODO better thro exception?!
  @Override public StreamNode<Void, ?> getChild(int i) {
    return null;
  }

}
