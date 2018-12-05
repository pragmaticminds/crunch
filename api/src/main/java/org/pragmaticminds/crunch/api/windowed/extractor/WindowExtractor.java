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

package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.windowed.WindowedEvaluationFunction;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Collection;

/**
 * Extracts resulting {@link GenericEvent}s from {@link MRecord}s for
 * {@link EvaluationFunction}s which are using this interface.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface WindowExtractor<T extends Serializable> extends Serializable {

    /**
     * This method collects single values for the later made extraction.
     * @param record from the eval call of the {@link WindowedEvaluationFunction}.
     */
    void apply(MRecord record);

    /**
     * Generates resulting {@link Collection} of {@link GenericEvent} from the applied {@link MRecord}s and
     * an {@link EvaluationContext} after am {@link EvaluationFunction} has met it's entry conditions.
     * @param context of the current eval call to the parent {@link EvaluationFunction}. also collects the resulting
     *                {@link GenericEvent}s
     */
    void finish(EvaluationContext<T> context);
}
