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
