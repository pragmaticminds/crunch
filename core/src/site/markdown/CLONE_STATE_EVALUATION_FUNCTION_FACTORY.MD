<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->
  
# CloneStateEvaluationFunctionFactory

This is a simple cloning implementation of the [EvaluationFunctionStateFactory](EVALUATION_FUNCTION_STATE_FACTORY.html) 
interface. In its constructor this class takes an original/prototype instance of a 
[EvaluationFunction](EVALUATION_FUNCTION.html) and clones it every time the method "create" is called.

## Code example

```
// create a simple EvaluationFunction
EvaluationFunction evaluationFunctionOriginal = (context) -> {
    if(context.get().getDouble("U") > 23.0){
        context.collect(
            context.getEventBuilder()
                .withEvent("U_IS_HIGHER_23_VOLTAGE")
                .withSource(context.get().getSource())
                .withTimestamp(context.get().getTimestamp())
                .withParameter("system time", System.getCurrentTimestamp())
                .build();
        );
    }
};

...

// create the factory
CloneStateEvaluationFunctionFactory factory = CloneStateEvaluationFunctionFactory.builder()
    .withPrototype(evaluationFunctionOriginal)
    .build();

...

EvaluationFunction currentInstance = Evalufactory.create();

```
In this example first an original instance of a [EvaluationFunction](EVALUATION_FUNCTION.html) is created.
This instance is passed to the Builder of the CloneStateEvaluationFunctionFactory and build is called, so that there is
a factory instance.

To create untouched instances of the [EvaluationFunction](EVALUATION_FUNCTION.html) the method "create" is called and the
new instance of the [EvaluationFunction](EVALUATION_FUNCTION.html) can be used for processing.