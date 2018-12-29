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
  
# TriggerStrategies
Library of static available [TriggerStrategy](TRIGGER_STRATEGY.html) implementations.

All of contained implementations require a [Supplier\<T\>](SUPPLIER.html) definition to extract the value of interest.

## Available strategies

- onTrue([Supplier\<Boolean\>](SUPPLIER.html) supplier) : Fires when the supplied value is "true".

- onFalse([Supplier\<Boolean\>](SUPPLIER.html) supplier) : Fires when the supplied value is "false".

- onBecomeTrue([Supplier\<Boolean\>](SUPPLIER.html) supplier): Fires when the supplied value is "true" and was "false" before.

- onBecomeFalse([Supplier\<Boolean\>](SUPPLIER.html) supplier): Fires when the supplied value is "false" and was "true" before.

- onChange([Supplier\<T\>](SUPPLIER.html) supplier): Fires when the supplied value is not the same as the one before.

- onNull([Supplier\<T\>](SUPPLIER.html)): Fires when the supplied channel is not available or set with a null value. 

- onNull([Supplier\<T\>](SUPPLIER.html)): Fires when the supplied channel is set and has an other value as null. 

- always(): Fires always. Does not need a [Supplier\<T\>](SUPPLIER.html).
