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
  
# EvaluationFunctionStateFactory (Interface)

This is a default [EvaluationFunction](EVALUATION_FUNCTION.html) factory. It has a create method, on which it creates a 
new or cloned instance of the [EvaluationFunction](EVALUATION_FUNCTION.html) of the factory.

## Available implementations :

- __[CloneStateEvaluationFunctionFactory](CLONE_STATE_EVALUATION_FUNCTION_FACTORY.html)__ a factory that takes an instance
of a [EvaluationFunction](EVALUATION_FUNCTION.html) as prototype and clones it every time the "create" method is called.