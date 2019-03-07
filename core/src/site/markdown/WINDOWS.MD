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
  
# Wiondows
This is a library collection class of implementations of [RecordWindow](RECORD_WINDOW.html). All contents of the library 
are statically available.

## Availabel implementations

- bitActive([Supplier\<T\>](SUPPLIER.html) supplier): window is open when the supplied value is "true".

- bitNotActive([Supplier\<T\>](SUPPLIER.html) supplier): window is open when the supplied value is "false".

- valueEquals([Supplier\<T\>](SUPPLIER.html) supplier, T expected): window is open when the supplied value is equal to the 
expected value.

- valueNotEquals([Supplier\<T\>](SUPPLIER.html) supplier, T notExpected): window is open when the supplied value is not 
equal to the notExpected value.