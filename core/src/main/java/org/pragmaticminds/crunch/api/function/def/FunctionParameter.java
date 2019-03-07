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

import org.pragmaticminds.crunch.api.records.DataType;

import java.io.Serializable;

/**
 * Represents an FunctionParameter of the Signature
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionParameter implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private FunctionParameterType parameterType;

    private DataType dataType;

    public FunctionParameter() {
        super();
    }

    public FunctionParameter(String name, FunctionParameterType parameterType, DataType dataType) {
        this.name = name;
        this.parameterType = parameterType;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public FunctionParameterType getParameterType() {
        return parameterType;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "FunctionParameter{" +
                "name='" + name + '\'' +
                ", parameterType=" + parameterType +
                ", dataType=" + dataType +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FunctionParameter parameter = (FunctionParameter) o;

        return getParameterType() == parameter.getParameterType() && getDataType() == parameter.getDataType();
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getParameterType() != null ? getParameterType().hashCode() : 0);
        result = 31 * result + (getDataType() != null ? getDataType().hashCode() : 0);
        return result;
    }
}
