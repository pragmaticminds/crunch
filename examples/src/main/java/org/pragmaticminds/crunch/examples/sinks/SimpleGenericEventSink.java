/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.examples.sinks;

import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.execution.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple print out implementation of the {@link EventSink} for {@link GenericEvent}s.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 */
public class SimpleGenericEventSink implements EventSink<GenericEvent> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleGenericEventSink.class);

    @Override
    public void handle(GenericEvent event) {
        logger.info("EventSink received: {}", event);
    }
}
