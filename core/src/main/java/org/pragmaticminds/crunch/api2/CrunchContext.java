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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CrunchContext {

  private List<Root<?>> streams = new ArrayList<>();

  public <E extends Serializable> Root<E> values(E... e) {
    return this.source(Linq4j.asEnumerable(e));
  }

  public <E extends Serializable> Root<E> source(Enumerable<E> values) {
    final Root<E> root = new Root<>(this, values);
    streams.add(root);
    return root;
  }

  public Future<Void> execute() {
    final List<Thread> threads = streams.stream()
        .map(s -> new Thread(getRunnable((Root<UntypedValues>) s)))
        .collect(Collectors.toList());

    // Start all threads
    threads.forEach(Thread::run);
    return CompletableFuture.completedFuture(null);
  }

  private Runnable getRunnable(Root<UntypedValues> stream) {
    return () -> {
      final Linq4jImplementor<UntypedValues, Serializable> implementor =
          new Linq4jImplementor<>(stream);

      final Enumerator<Void> enumerator = implementor.implement();

      while (enumerator.moveNext()) {
        // Do nothing
      }
    };
  }

}
