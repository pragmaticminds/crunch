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
  
#WindowedEvaluationFunction
This is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html).

## Challange :
The situation where an implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html) is done only to aggregate some 
typical values is a common one. This leads to the requirement for a default structure that already has the logic needed 
to aggregate channel values of interest on MRecords.

## Soulution :

The WindowedEvaluationFunction. It can aggregate values from channels in a window defined by the 
[RecordWindow](RECORD_WINDOW.html). This class has three constructs to support its functionality:

- [RecordWindow](RECORD_WINDOW.html) : determines when a aggregation phase starts and ends.

- [WindowExtractor](WINDOW_EXTRACTOR.html) : extracts the values of interest in resulting [Event](EVENT.html)s.

- [EventFilter](EVENT_FILTER.html) : (optional) filters the resulting events, so that only relevant [Event](EVENT.html)s are processed 
further.

## Code Example :

Instantiation:
```
WindowedEvaluationFunction function = WindowedEvaluationFunction.builder()
    .recordWindow(Windows.bitActive(booleanChannel("flag"))
    .extractor(new MaxWindowExtractor())
    .filter((event, record) -> event.getParameter("name").asString().equals("Mixer17"))
    .build();
```

In this case the [RecordWindow](RECORD_WINDOW.html) is watching on the Boolean "flag" channel on the records. As long this 
value is "true", the window is open. As soon as the window closes, the [WindowExtractor](WINDOW_EXTRACTOR.html) kicks in. 
Here the MaxWindowExtractor implementation in use. It aggregates the maximal value on a channel internally defined in
the class. It creates than an Event which holds the max value as a parameter. Than the [EventFilter](EVENT_FILTER.html) is 
activated, which looks out for the parameter "name" and compares it with the String "Mixer17".

#### Execution :

Since the WindowedEvaluationFunction is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html), it can be 
executed as such. (see [EvaluationFunction](EVALUATION_FUNCTION.html))