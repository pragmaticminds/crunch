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
  
# Readme

[![Build Status](https://travis-ci.com/pragmaticminds/crunch.svg?branch=develop)](https://travis-ci.com/pragmaticminds/crunch)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.pragmaticminds.crunch/crunch-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.pragmaticminds.crunch/crunch-parent)
[![Apache License, Version 2.0](https://img.shields.io/github/license/apache/maven.svg?label=License)](https://img.shields.io/github/license/apache/maven.svg?label=License)

Welcome to CRUNCH an industrial streaming data analysis framework built by [pragmatic minds GmbH](www.pragmaticminds.de).

## About

When dealing with data streams from industrial applications, e.g., machines there are often times 
very similar questions and processing steps necessary.
Think, e.g., of filtering or joining data from different sources. Futhermore, more complex tasks are
differentiation of signals (to monitor changes) or even application of Fourrier transformation 
(or related wavelet transformation).
These signals can then be analysed with regards to a set of built-in functions or custom functions.
What all of the functions have in common is, that you always have the temporal context of a datapoint.
Thus, it is easy to ask 

> When did this bit change from false to true?

or things like

> When is the steepness of this signal larger than ... for more than ... seconds

or 

> Emit an event each time when the signal is above ...

### What makes CRUNCH different from other Frameworks like Apache Flink, Apache Spark, Akka Streams, ...

There are many open source frameworks for stream processing. The main difference between them and CRUNCH is
that CRUNCH is very focused about it's application in _signal processing_ and related tasks and no
general streaming framework. Futhermore, as this kind of analysis is often done on the edge CRUNCH is not
very focused on scaling and fault tolerance in specific situations as this is not (that) relevant for edge devices.

## Examples

tbd.

### Contact

Obviuously, this readme is still beeing populated and we are still setting up our infrastructure (after open sourcing CRUNCH).
So if you have any questions please feel free to ask one of the commiters or write an email to [Julian](mailto:j.feinauer@pragmaticminds.de).