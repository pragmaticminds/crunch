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
  
# Supplier\<T\> (Interface)

## Use Case
Implementation of this interface are used to extract a channel value of interest from a MRecord.

## Lambda example implementations
#### Simple example on a Boolean channel

```
Supplier<Boolean> supplier = (Supplier<Boolean>)(values) -> values.getBoolean("flag");
```

In this case the supplier implementation is extracting the Boolean channel value with the name "flag".

#### Example on a Double channel

```
Supplier<Double> supplier = (Supplier<Double>)(values) -> values.getDouble("U");
```

In this case the supplier implementation is extracting the Double channel value with the name "U".

#### Example on two values at once

```
Supplier<Tuple2<Double, Boolean>> supplier = (Supplier<Tuple2<Double, Boolean>>) 
    (values) -> new Tuple2(values.getBoolean("flag"), values.getDouble("U"));
```

In this case the supplier implementation is extraction the both values from the previous examples at once.
It also could have returned a List or similar.

## Available implementations

- [Suppliers](SUPPLIERS.html) is a collection of typicle use case implementations.