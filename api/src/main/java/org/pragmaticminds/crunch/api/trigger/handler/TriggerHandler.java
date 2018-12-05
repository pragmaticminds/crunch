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

package org.pragmaticminds.crunch.api.trigger.handler;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;

/**
 * This class handles the situation when a {@link TriggerEvaluationFunction} is triggered.
 * It creates a proper result to the triggering.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
@FunctionalInterface
public interface TriggerHandler<T extends Serializable> extends Serializable {

    /**
     * When a {@link TriggerEvaluationFunction} is triggered, it calls this method to generate a proper result.
     *
     * @param context of the current processing. It holds the current MRecord and it takes the resulting {@link Event}
     *                objects.
     */
    void handle(EvaluationContext<T> context);
}
