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
  
# ChainedEvaluationFunction

This is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html).

## Challenge :
The situation where an implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html) is done to simulate a linear 
statemachine is a common one. This leads to the requirement for a default structure that already has the logic needed 
to apply a statemachine structure with linear processing/state changing.

## Solution :
The ChainedEvaluationFunction. It holds child implementations of the [EvaluationFunction](EVALUATION_FUNCTION.html), which
are representing single steps in the processing. This implementations are build by a factory that must fulfill a 
specific interface, the [EvaluationFunctionStateFactory](EVALUATION_FUNCTION_STATE_FACTORY.html). This structure has the 
following helping sub structures:

- __List of [EvaluationFunctionStateFactory](EVALUATION_FUNCTION_STATE_FACTORY.html)__ ( + state timeout) : for each 
turnaround of the statemachine, the factories are creating a fresh instance of the state 
[EvaluationFunction](EVALUATION_FUNCTION.html). When one of this [EvaluationFunction](EVALUATION_FUNCTION.html) is executed
and it returns resulting [Event](EVENT.html), than this means the switch to the next state is reached. When a timeout is 
set for a state and the timeout is raised while execution, than the whole statemachine is reset.

- __over all timeout__ : This timeout can be set for the entire processing duration of the statemachine. When the timout 
is raised, the whole statemachine is reset.

- __[StateErrorExtractor](STATE_ERROR_EXTRACTOR.html)__ : extracts a resulting [Event](EVENT.html), when a timeout was raised.

- __[ChainedProcessingCompleteExtractor](CHAINED_PROCESSING_COMPLETE_EXTRACTOR.html)__ : extracts resulting 
[Event](EVENT.html)s, after all states have been successfully processed. 

## Code Example :

#### Instantiation:

```
Long overallTimeoutMs = 2400;

// the Tuple2 instances in the List are holding the EvaluationFunction implementations 
// and the state timeout for each state
List<Tuple2<EvaluationFunctionStateFactory, Long>> stateFactories = new ArrayList<>();

...

// here a previously created result Event is used, in generall the user has to build an own Event as result
StateErrorExtractor errorExtractor = 
    (StateErrorExtractor) (events, ex, context) -> context.collect(errorEvent);

// here a previously created result Event is used, in generall the user has to build an own Event as result
ChainProcessingCompleteExtractor stateCompleteExtractor = 
    (ChainProcessingCompleteExtractor) (events, context) -> context.collect(completeEvent);

...

ChainedEvaluationFunction function = ChainedEvaluationFunction.builder()
    .withStateFactoriesAndTimeouts(stateFactories)
    .withOverallTimeoutMs(overallTimeoutMs)
    .withErrorExtractor(errorExtractor)
    .withStateCompleteExtractor(stateCompleteExtractor)
    .build();
    
```



#### Execution :

Since the ChainedEvaluationFunction is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html), it can be 
executed as such. (see [EvaluationFunction](EVALUATION_FUNCTION.html))