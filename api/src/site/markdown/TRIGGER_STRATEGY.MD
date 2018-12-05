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
  
# TriggerStrategy (Interface)

## Use Case
Implementations of this interface are used to determine if a condition for triggering the extraction process in a 
[TriggerEvaluationFunction](TEF.html) is to be executed.

## Lambda example implementations

#### Simple example on a Boolean channel
```
TriggerStrategy strategy = (TriggerStrategy)(values) -> values.getBoolean("flag");
```

In this case the trigger is fired, when an MRecord has the channel "flag" as boolean and its value is "true".

#### Example on a Double channel

```
TriggerStrategy strategy = (TriggerStrategy)(values) -> values.getDouble("U") > 320.0;
```

In this case the trigger is fired, when an MRecord has the channel "U" as Double and its value is greater than 320.

## Available implementations

- [TriggerStrategies](TRIGGER_STRATEGIES.html) hold a collection of typical implementations.