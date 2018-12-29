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
  
# ChainProcessingCompleteExtractor (Interface)

With implementations of this interface resulting [Event](EVENT.html)s are created, when a 
[ChainedEvaluationFunction](CEF.html) is completing its round trip. All result [Event](EVENT.html)s from the inner
state [EvaluationFunction](EVALUATION_FUNCTION.html)s of the [ChainedEvaluationFunction](CEF.html) are passed to its 
"process" method, together with the [EvaluationContext](EVALUATION_CONTEXT.html), where this class can publish its own
resulting [Event](EVENT.html)s, which it must self create.

## Code example :

```
ChainedProcessingCompleteExtractor extractor = (ChainProcessingCompleteExtractor) (events, context) -> 
    context.collect(
        context.getEventBuilder()
            .withEvent("NAME_OF_RESULT_EVENT")
            .withSource(context.get().getSource())
            .withTimestamp(context.get().getTimestamp())
            .withParameter("state1Result", events.get("state1").getParameter("result"))
            .withParameter("state2Result", events.get("state2").getParameter("result"))
            .withParameter("state3Result", events.get("state3").getParameter("result"))
            .withParameter("state4Result", events.get("state4").getParameter("result"))
            .build();    
    );
```

In this example the [ChainedEvaluationFunction](CEF.html) has at least 4 states, which are returning [Event](EVENT.html)s 
with the names "state1Result", "state2Result" ...

A resulting [Event](EVENT.html) is being created with the 4 results of the state 
[EvaluationFunction](EVALUATION_FUNCTION.html)s result [Event](EVENT.html)s.

The resulting [Event](EVENT.html) is passed to the context.