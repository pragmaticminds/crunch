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

package org.pragmaticminds.crunch.api.function.def;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.annotations.ResultType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entity to save Results fired from an {@link EvalFunction} in processing
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.05.2017.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<FunctionResult> results;

    public FunctionResults() {
        super();
    }

    public FunctionResults(ResultType[] resultTypes) {
        this.results = new ArrayList<>();
        Arrays.stream(resultTypes).forEach(rt -> results.add(new FunctionResult(rt.name(), rt.dataType())));
    }

    public FunctionResults(List<FunctionResult> results) {
        this.results = results;
    }

    public List<FunctionResult> getResults() {
        return results;
    }

    public void setResults(List<FunctionResult> results) {
        this.results = results;
    }
}
