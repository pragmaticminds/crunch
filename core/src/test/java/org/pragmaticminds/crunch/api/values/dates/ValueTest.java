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

package org.pragmaticminds.crunch.api.values.dates;

import org.junit.Test;

import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 * @author kerstin
 * Created by kerstin on 06.11.17.
 */
public class ValueTest {

    @Test
    public void ofObject_boolean() {
        Object o = Boolean.FALSE;
        Value of = Value.of(o);
        assertFalse(of.getAsBoolean());
    }

    @Test
    public void ofObject_string() {
        Object o = "test";
        Value of = Value.of(o);
        assertEquals("test", of.getAsString());
    }

    @Test
    public void ofObject_date() {
        Date date = Date.from(Instant.now());
        Value of = Value.of((Object) date);
        assertEquals(date, of.getAsDate());
    }

    @Test
    public void ofObject_float() {
        Object o = 3.14f;
        Value of = Value.of(o);
        assertEquals(3.14, of.getAsDouble(), 0.001);
    }

    @Test
    public void ofObject_double() {
        Object o = 3.14;
        Value of = Value.of(o);
        assertEquals(3.14, of.getAsDouble(), 0.001);
    }

    @Test
    public void ofObject_byte() {
        Object o = (byte) 42;
        Value of = Value.of(o);
        assertEquals(new Long(42L), of.getAsLong());
    }

    @Test
    public void ofObject_short() {
        Object o = (short) 42L;
        Value of = Value.of(o);
        assertEquals(new Long(42L), of.getAsLong());
    }

    @Test
    public void ofObject_long() {
        Object o = 42L;
        Value of = Value.of(o);
        assertEquals(new Long(42L), of.getAsLong());
    }

    @Test(expected = InvalidParameterException.class)
    public void ofObject_fails() {
        Object o = Instant.now();
        Value.of(o);
    }

    @Test
    public void ofNull() {
        Object o = null;
        Value.of(o);
    }

}