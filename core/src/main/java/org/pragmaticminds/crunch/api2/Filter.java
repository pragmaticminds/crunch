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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Node that represents a filter operation by the given predicate.
 *
 * @param <E> Type of the elements
 */
public class Filter<E> extends AbstractStreamNode<E, E> {

  final Predicate<E> predicate;

  public Filter(Predicate<E> predicate) {
    super();
    this.predicate = predicate;
  }

  @Override public <T> T accept(StreamNodeVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public <KEY> GroupBy<E, KEY> groupBy(Function<E, KEY> groupAssigner) {
    final GroupBy<E, KEY> groupBy = new GroupBy<>(groupAssigner);
    this.addChild(groupBy);
    return groupBy;
  }

}
