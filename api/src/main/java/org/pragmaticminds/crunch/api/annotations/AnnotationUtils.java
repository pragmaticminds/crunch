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

package org.pragmaticminds.crunch.api.annotations;

import org.pragmaticminds.crunch.api.AnnotatedEvalFunction;
import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for working with annotated classes
 *
 * @author julian
 * Created by julian on 04.04.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class AnnotationUtils {

    private static final Logger logger = LoggerFactory.getLogger(AnnotationUtils.class);

    private AnnotationUtils() {
        /* for Sonar */
    }

    public static List<Field> getValuesFromAnnotatedType(Object instance, Class<? extends Annotation> annotation) {
        return findFields(instance.getClass(), annotation);
    }

    public static void setValuesToAnnotatedType(List<Object> values, Object instance, Class<? extends Annotation> annotation) throws IllegalAccessException {
        List<Field> fields = findFields(instance.getClass(), annotation);
        for (int i = 0; i < Math.min(fields.size(), values.size()); i++) {
            fields.get(i).setAccessible(true);
            fields.get(i).set(instance, values.get(i));
        }
    }

    /**
     * Finds all methods in the given class that are annotated with the given annotation.
     *
     * @param annotation Annotation to find
     * @param classs Class to search through
     * @return null safe set
     */
    public static List<Field> findFields(Class<?> classs, Class<? extends Annotation> annotation) {
        List<Field> set = new ArrayList<>();
        Class<?> c = classs;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(annotation)) {
                    set.add(field);
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }

    /**
     * Injects the {@link GenericEventHandler} object into an {@link AnnotatedEvalFunction}
     *
     * @param object       the target object where to the {@link GenericEventHandler} is to be injected
     * @param eventHandler the one that is to be injected into the target object
     */
    public static void injectEventStream(Object object, GenericEventHandler eventHandler) {
        for (Field field : object.getClass().getDeclaredFields()) {
            if (field.getType().equals(GenericEventHandler.class)) {
                field.setAccessible(true);
                try {
                    field.set(object, eventHandler);
                } catch (IllegalAccessException e) {
                    logger.error("Error for injecting event stream! {}", e);
                }
            }
        }
    }
}
