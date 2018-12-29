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

package org.pragmaticminds.crunch.api.holder;

import org.pragmaticminds.crunch.api.records.DataType;

import java.io.Serializable;

/**
 * This class is the basis for all holders (for all primitives / {@link DataType}.
 * Holders "hold" their Value to pass it into the Evaluation Function ({@see Holder Pattern}.
 * <p>
 * This class is Serializable but does not serialize it's current Value!
 * This is not necessary as it always "resettet" during the Evaluation.
 *
 * @author julian
 * Created by julian on 17.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class Holder<T> implements Serializable {

    private transient T value;
    private Class type;

    public Holder(T value, Class type) {
        this.value = value;
        this.type = type;
    }

    public Holder(Class type) {
        this.value = null;
        this.type = type;
    }

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return this.value;
    }

    public Class getType() {
        return type;
    }

}
