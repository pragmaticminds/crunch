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

package org.pragmaticminds.crunch.api.evaluations;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.pragmaticminds.crunch.api.function.def.FunctionDef;
import org.pragmaticminds.crunch.api.function.def.FunctionParameter;
import org.pragmaticminds.crunch.api.function.def.FunctionParameterType;
import org.pragmaticminds.crunch.api.function.def.FunctionResult;
import org.pragmaticminds.crunch.api.function.def.FunctionResults;
import org.pragmaticminds.crunch.api.function.def.Signature;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Searches in the set channel for the regex (find and not match)
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 18.10.2017
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class RegexFind extends EvalFunction<String> {

    private Pattern pattern;

    /**
     * @return function properties of this class
     */
    @Override
    public FunctionDef getFunctionDef() {
        return new FunctionDef(
                new Signature(
                        "REGEX_FIND",
                        new FunctionParameter(
                                "regex",
                                FunctionParameterType.LITERAL,
                                DataType.STRING
                        ),
                        new FunctionParameter(
                                "value",
                                FunctionParameterType.CHANNEL,
                                DataType.STRING
                        )
                ),
                DataType.STRING,
                this.getClass(),
                new FunctionResults(Arrays.asList(
                        new FunctionResult("match found", DataType.STRING),
                        new FunctionResult("found in", DataType.STRING)
                ))
        );
    }

    /**
     * initialises this function
     *
     * @param literals     the constant in values
     * @param eventHandler the interface to fire results into the system
     */
    @Override
    public void setup(Map<String, Value> literals, GenericEventHandler eventHandler) {
        String regexString = literals.get("regex").getAsString();
        setEventHandler(eventHandler);
        pattern = Pattern.compile(regexString);
    }

    /**
     * processes single records
     *
     * @param channels contains the values of a record
     * @return output of this function. If found the input value of the channel, if not than null
     */
    @Override
    public String eval(long timestamp, Map<String, Value> channels) {
        String value = channels.get("value").getAsString();
        Matcher matcher = pattern.matcher(value);
        GenericEventHandler handler = getEventHandler();
        if (matcher.find()) {
            handler.fire(
                    handler.getBuilder()
                            .withTimestamp(Instant.now().toEpochMilli())
                            .withEvent("type")
                            .withSource("none")
                            .withParameter("match found", Value.of(matcher.group()))
                            .withParameter("found in", Value.of(value))
                            .build());
            return value;
        }

        return null;
    }

    /**
     * nothing to cleanup
     */
    @Override
    public void finish() {
        // do nothing
    }
}
