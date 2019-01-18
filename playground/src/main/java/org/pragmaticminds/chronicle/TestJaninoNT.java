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

package org.pragmaticminds.chronicle;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestJaninoNT {

  public static void main(String[] args) throws CompileException, ClassNotFoundException, IllegalAccessException, InstantiationException {

  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(2)
  @Warmup(iterations = 20)
  @Measurement(iterations = 20)
  public void run() throws IllegalAccessException, InstantiationException, ClassNotFoundException, CompileException {
    final HashSet<String> filter = new HashSet<>();
    for (int i = 1; i <= 100; i++) {
      filter.add(UUID.randomUUID().toString());
    }

    // Now generate things to filter
    for (int i = 1; i <= 1_000; i++) {
      final HashMap<String, Object> map = new HashMap<>();
      for (int j = 1; j <= 50; i++) {
        map.put(UUID.randomUUID().toString(), "");
      }
      // Now check if entries ae in the filter
    }

    final SimpleCompiler compiler = new SimpleCompiler();
    System.out.println(MyInt.class.getCanonicalName());
    compiler.cook("import org.pragmaticminds.chronicle.TestJaninoNT;" +
        "public class Asdf implements TestJaninoNT.MyInt { " +
        "@Override public void doSomething() {" +
        "System.out.println(\"Hallo\");" +
        "}" +
        "}");
    final Class<MyInt> asdf = (Class<MyInt>) Class.forName("Asdf", true, compiler.getClassLoader());
    final MyInt myInt = asdf.newInstance();

    System.out.println(myInt.getClass());
    for (int i = 1; i <= 1000; i++) {
      myInt.doSomething();
    }
  }

  public interface MyInt {

    void doSomething();

  }
}
