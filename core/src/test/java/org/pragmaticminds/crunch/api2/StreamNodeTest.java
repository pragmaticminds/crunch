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

import java.io.Serializable;

public class StreamNodeTest {

  public static <I, O> StreamNode<I, O> add(StreamNode<?, I> parent, StreamNode<I, O> child) {
    parent.addChild(child);
    return child;
  }

  @Test
  public void create() throws NoSuchMethodException {
    final CrunchContext context = new CrunchContext();
    final Root<Integer> root = new Root<>(context, null);
    final Filter<Integer> filter = new Filter<>(i -> (i % 5) == 0);
    root.addChild(filter);
    final GroupBy<Integer, Integer> groupBy = new GroupBy<>(i -> i % 5);
    filter.addChild(groupBy);
    final Evaluate<Integer, Serializable> evaluate = new Evaluate<>(null);
    groupBy.addChild(evaluate);
    final ResultHandler<Serializable> handler = new ResultHandler<>(this, StreamNodeTest.class.getMethod("create"));
    evaluate.addChild(handler);

    final StreamNodeVisitor<Void> vis = new StreamNodeVisitor<Void>() {


      @Override public <IN, EVENT extends Serializable> Void visit(Evaluate<IN, EVENT> evaluate) {
        System.out.println("Evaluate function " + evaluate.getEvaluation());
        for (int i = 0; i < evaluate.getChildren().size(); i++) {
          evaluate.getChild(i).accept(this);
        }
        return null;
      }

      @Override public <EVENT> Void visit(ResultHandler<EVENT> handler) {
        System.out.println("Handle result " + handler.getInstance().getClass().getName() + "#" + handler.getMethod().getName() + " on " + handler.getInstance());
        for (int i = 0; i < handler.getChildren().size(); i++) {
          handler.getChild(i).accept(this);
        }
        return null;
      }

      @Override public Void visit(GroupBy groupBy) {
        System.out.println("Group By " + groupBy.groupAssigner);
        for (int i = 0; i < groupBy.getChildren().size(); i++) {
          groupBy.getChild(i).accept(this);
        }
        return null;
      }

      @Override public Void visit(Filter filter) {
        System.out.println("Filter with " + filter.predicate);
        for (int i = 0; i < filter.getChildren().size(); i++) {
          filter.getChild(i).accept(this);
        }
        return null;
      }

      @Override public Void visit(Root root) {
        System.out.println("Root with context " + root.context);
        for (int i = 0; i < root.getChildren().size(); i++) {
          root.getChild(i).accept(this);
        }
        return null;
      }

    };

    root.accept(vis);
  }
}