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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class ResultHandler<EVENT> implements StreamNode<EVENT, Void> {

  private final Object instance;
  private final Method method;

  public ResultHandler(Object instance, Method method) {
    this.instance = instance;
    this.method = method;
  }

  public Object getInstance() {
    return instance;
  }

  public Method getMethod() {
    return method;
  }

  public Consumer<EVENT> getConsumer() {
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
