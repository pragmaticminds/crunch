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

import org.apache.calcite.linq4j.Enumerable;

public class Root<OUT> extends AbstractStreamNode<Void, OUT> {

  final CrunchContext context;
  final Enumerable<OUT> values;

  public Root(CrunchContext context, Enumerable<OUT> values) {
    super();
    this.context = context;
    this.values = values;
  }

  public CrunchContext getContext() {
    return context;
  }

  public Enumerable<OUT> getValues() {
    return values;
  }

  @Override public <T> T accept(StreamNodeVisitor<T> visitor) {
    return visitor.visit(this);
  }

}
