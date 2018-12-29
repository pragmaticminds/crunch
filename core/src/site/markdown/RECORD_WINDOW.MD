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
  
# RecordWindow (Interface)
This interface has a "inWindow(MRecord record)" method. The implementations of this must determine if the window is
open or closed. While a window is open the [WindowEvaluationFunction](WEF.html) aggregates incoming records, where it
extracts the channel values of interest of the MRecords.

## Simple implementation example

```
RecordWindow window = (record) -> record.getBoolean("flag");
```

In this example the window is always open when the Boolean channel "flag" has the value "true".

```
RecordWindow window = (record) -> record.getString("id").equals("Mixer17");
``` 

In this example the window is always open when the String channel "id" has the value "Mixer17".

##Implementations for typical use cases

- [Windows](WINDOWS.html) a library of static implementations of this interface.