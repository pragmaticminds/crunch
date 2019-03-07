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
 * This represents the definition of an Evaluation Function.
 * It consists of a name of the function and a signature and a return type.
 * <p>
 * Created by julian on 14.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionDef implements Serializable {

    private DataType outputDataType;

    private String className;

    private Signature signature;

    private FunctionResults functionResults;

    public FunctionDef() {
        super();
    }

    public FunctionDef(Signature signature, Class evaluationFunctionClass, FunctionResults functionResults) {
        this.signature = signature;
        this.className = evaluationFunctionClass.getName();
        this.functionResults = functionResults;
        outputDataType = null;
    }

    public FunctionDef(Signature signature, DataType outputDataType,
                       Class evaluationFunctionClass, FunctionResults functionResults) {
        this.signature = signature;
        this.functionResults = functionResults;
        this.outputDataType = outputDataType;
        this.className = evaluationFunctionClass.getName();
    }

    public void setFunctionResults(FunctionResults functionResults) {
        this.functionResults = functionResults;
    }

    public String getClassName() {
        return this.className;
    }

    public Signature getSignature() {
        return signature;
    }

    public DataType getOutputDataType() {
        return outputDataType;
    }

    public void setOutputDataType(DataType outputDataType) {
        this.outputDataType = outputDataType;
    }

    public Class getEvaluationFunctionClass() throws ClassNotFoundException {
        return Class.forName(className);
    }

    @Override
    public String toString() {
        return "FunctionDef{" +
                "signature=" + signature +
                ", functionResults=" + functionResults +
                ", outputDataType=" + outputDataType +
                ", className='" + className + '\'' +
                '}';
    }
}
