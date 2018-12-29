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
  
# EvaluationFunction (Interface)

This interface represents the main processing entity in the Crunch environment. It is the interface of all 
EvaluationFunction implementations. It can be implemented as a lambda, because it has a "@FunctionalInterface" 
annotation. It has a eval function for processing an [EvaluationContext](EVALUATION_CONTEXT.html), which has an incoming 
value and handles outgoing results for the EvaluationFunction.


## Common Use Case Implementations

[TEF - TriggeredEvaluationFunction](TEF.html)

[WEF - WindowedEvaluationFunction](WEF.html)