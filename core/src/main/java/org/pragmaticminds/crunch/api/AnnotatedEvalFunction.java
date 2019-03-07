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

package org.pragmaticminds.crunch.api;

import java.io.Serializable;

/**
 * A special way to define {@link EvalFunction}s. Definition is made by annotation.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.10.2017
 *
 * @deprecated Part of the old API
 */
@Deprecated
public interface AnnotatedEvalFunction<T> extends Serializable {
    /**
     * is called before the processing of records begins, to initialize the inner structure of the {@link EvalFunction}
     */
    void setup();

    /**
     * processes a single record and returns an output value of the set type
     *
     * @return output value of set type
     */
    T eval();

    /**
     * is called after processing of the records is finished
     */
    void finish();

    /**
     * This default function wrapps an {@link AnnotatedEvalFunction} as an {@link EvalFunction}, so that it can be
     * treated as one.
     *
     * @return this wrapped {@link AnnotatedEvalFunction} as an {@link EvalFunction}
     * @throws IllegalAccessException should not happen, when happens, than some classes are not visible from the
     *                                current context
     */
    default EvalFunction<T> asEvalFunction() throws IllegalAccessException {
        return new AnnotatedEvalFunctionWrapper(this.getClass());
    }
}
