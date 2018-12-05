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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class represents the signature of an evaluation function or {@link FunctionDef}.
 * It contains the Arguments expected with their types, their names (if named attributes will be supported one day)
 * and their order.
 * <p>
 * Created by julian on 14.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class Signature implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private List<FunctionParameter> parameters;

    public Signature() {
        super();
    }

    public Signature(String name, FunctionParameter arguments) {
        this.name = name;
        this.parameters = Collections.singletonList(arguments);
    }

    public Signature(String name, FunctionParameter... parameters) {
        this.name = name;
        this.parameters = Arrays.asList(parameters);
    }

    public Signature(String name, List<FunctionParameter> parameterList) {
        this.name = name;
        this.parameters = parameterList;
    }

    public String getName() {
        return name;
    }

    public FunctionParameter[] getParameters() {
        return parameters.toArray(new FunctionParameter[parameters.size()]);
    }

    public FunctionParameter getArgument(int i) {
        return parameters.get(i);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name + "(");
        for (FunctionParameter parameter : parameters) {
            sb.append("<").append(parameter.getParameterType().toString()).append(":").append(parameter.getDataType().toString()).append(">,");
        }
        return sb.substring(0, sb.length() - 1) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Signature signature = (Signature) o;

        if (getName() != null ? !getName().equals(signature.getName()) : signature.getName() != null) {
            return false;
        }

        if (getParameters().length != signature.getParameters().length) {
            return false;
        }

        for (int i = 0; i < getParameters().length; i++) {
            if (!getParameters()[i].equals(signature.getParameters()[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + Arrays.hashCode(getParameters());
        return result;
    }
}
