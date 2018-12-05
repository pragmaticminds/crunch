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

package org.pragmaticminds.crunch.api.values.dates;

/**
 * This Visitor implements the superior and beloved Visitor Pattern.
 * It is usefull when someone wants to transform {@link Value}'s into something else and has to react to each
 * subclass of {@link Value} differently.
 * <p>
 * Has to be extended for all new subclasses of {@link Value}.
 *
 * @author julian
 * Created by julian on 13.11.17
 */
public interface ValueVisitor<T> {

    T visit(BooleanValue value);

    T visit(DateValue value);

    T visit(DoubleValue value);

    T visit(LongValue value);

    T visit(StringValue value);

}
