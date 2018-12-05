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

package org.pragmaticminds.crunch.api.records;

import java.security.InvalidParameterException;
import java.time.Instant;

/**
 * Data Types for Signal Data.
 * Created by julian on 06.07.16.
 */
public enum DataType {

    BOOL(Boolean.class),
    BYTE(Byte.class),
    INT_32(Integer.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    TIMESTAMP(Instant.class),
    STRING(String.class);

    private final Class<?> classType;

    DataType(Class<?> classType) {
        this.classType = classType;
    }

    public static DataType fromJavaType(Class<?> clazz) {
        if (clazz == String.class) {
            return STRING;
        } else if (clazz == Instant.class) {
            return TIMESTAMP;
        } else if (clazz == double.class || clazz == Double.class) {
            return DOUBLE;
        } else if (clazz == long.class || clazz == Long.class) {
            return LONG;
        } else if (clazz == int.class || clazz == Integer.class) {
            return INT_32;
        } else if (clazz == byte.class || clazz == Byte.class) {
            return BYTE;
        } else if (clazz == boolean.class || clazz == Boolean.class) {
            return BOOL;
        } else {
            throw new InvalidParameterException("The Class " + clazz + " cannot be mapped onto Date Type!");
        }
    }

    public Class<?> getClassType() {
        return classType;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase();
    }
}
