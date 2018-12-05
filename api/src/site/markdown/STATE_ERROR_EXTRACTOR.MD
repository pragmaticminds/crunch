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
  
# StateErrorExtractor (Interface)

Implementations of this interface are used to generate error result [Event](EVENT.html)s. When one of the timeouts in the
[ChainedEvaluationFunction](CEF.html) was raised, the implementation of this interface is called.

## Code example :

```
StateErrorExtractor extractor = (StateErrorExtractor) (events, ex, context) -> 
    context.collect(
        context.getEventBuilder()
            .withEvent("NAME_OF_ERROR_RESULT_EVENT")
            .withSource(context.get().getSource())
            .withTimestamp(context.get().getTimestamp())
            .withParameter("exception", ex == null ? "none" : ex.getMessage())
            .withParameter("state1Result", events.get("state1") == null ? "none" : events.get("state1").getParameter("result"))
            .withParameter("state2Result", events.get("state2") == null ? "none" : events.get("state2").getParameter("result"))
            .withParameter("state3Result", events.get("state3") == null ? "none" : events.get("state3").getParameter("result"))
            .withParameter("state4Result", events.get("state4") == null ? "none" : events.get("state4").getParameter("result"))
            .build();    
    );
```

When there was a exception, its message is set in the resulting [Event](EVENT.html), otherwise the value "none" is entered.

In this example the [ChainedEvaluationFunction](CEF.html) has at least 4 states, which are returning [Event](EVENT.html)s 
with the names "state1Result", "state2Result" ...

When a state was successful executed the result is not null and can be set, otherwise the value "none" is set.

The resulting error [Event](EVENT.html) is passed to the context.