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
  
# Event
The main result data object for the processing in an [EvaluationFunction](EVALUATION_FUNCTION.html) implementation.

It should only be created by the in a [EvaluationContext](EVALUATION_CONTEXT.html) available Builder. So that default 
values can already be set from the context. (not jet implemented that way, still this rule is to be followed!!!)

## Instantiation

A typical instatiation situation is when a [EvaluationContext](EVALUATION_CONTEXT.html) is in the scope.

```
Event event = context.getEventBuilder()
    // set the name
    .withEvent("nameOfTheEvent")
    // set the source
    .withSource(mRecord.getSource())
    // set the timestamp    
    .withTimestamp(mRecord.getTimestamp())
    // set the parameters
    .withParameter("U", Value.of(mRecord.get("U"))
    .withParameter("I", Value.of(mRecord.get("I"))
    .withParameter("T", Value.of(mRecord.get("T"))
    // let the Builder biuld the Event
    .build()
```