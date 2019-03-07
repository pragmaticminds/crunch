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

package org.pragmaticminds.crunch.examples.trigger;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.examples.sources.ValuesGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * This class demonstrates how Suppliers are to be used
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 *
 * SuppressWarnings : squid:S1192 : no need to define constant members for redundant string values : it does not help
 * understanding the source code.
 */
@SuppressWarnings("squid:S1192")
public class SuppliersExamples {
    private static final Logger logger = LoggerFactory.getLogger(SuppliersExamples.class);
    private static final List<Map<String, Object>> values = ValuesGenerator.generateMixedData(1, "channel_");
    private static final MRecord record = UntypedValues.builder().prefix("").source("testSource").values(values.get(0)).timestamp(Instant.now().toEpochMilli()).build();

    /** hidden constructor*/ private SuppliersExamples() { /* do nothing */}

    public static class DoubleChannelExtractorExample {
        public static void main(String[] args) {
            Supplier<Double> supplier = Suppliers.ChannelExtractors.doubleChannel("channel_double");
            Double result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class LongChannelExtractorExample {
        public static void main(String[] args) {
            Supplier<Long> supplier = Suppliers.ChannelExtractors.longChannel("channel_long");
            Long result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class BooleanChannelExtractorExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.ChannelExtractors.booleanChannel("channel_booleanTrue");
            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class DateChannelExtractorExample {
        public static void main(String[] args) {
            Supplier<Date> supplier = Suppliers.ChannelExtractors.dateChannel("channel_date");
            Date result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class StringChannelExtractorExample {
        public static void main(String[] args) {
            Supplier<String> supplier = Suppliers.ChannelExtractors.stringChannel("channel_string");
            String result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class GreaterThatWithConstantComparatorExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.Comparators.greaterThan(-200L, Suppliers.ChannelExtractors.longChannel("channel_long"));
            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class GreaterThatWithTwoSuppliersComparatorExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.Comparators.greaterThan(
                    Suppliers.ChannelExtractors.longChannel("channel_longNegative"),
                    Suppliers.ChannelExtractors.longChannel("channel_long")
            );
            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class ParseLongExample {
        public static void main(String[] args) {
            Supplier<Long> supplier = Suppliers.Parser.parseLong(Suppliers.ChannelExtractors.stringChannel("channel_stringLongNumber"));
            Long result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class NotBooleanOperatorsExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.BooleanOperators.not(Suppliers.ChannelExtractors.booleanChannel("channel_booleanFalse"));
            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class AndBooleanOperatorsExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.BooleanOperators.and(
                    Suppliers.BooleanOperators.not(Suppliers.ChannelExtractors.booleanChannel("channel_booleanFalse")),
                    Suppliers.ChannelExtractors.booleanChannel("channel_booleanTrue")
            );

            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class AddMathematicsExample {
        public static void main(String[] args) {
            Supplier<Double> supplier = Suppliers.Mathematics.add(123L, Suppliers.ChannelExtractors.doubleChannel("channel_double"));

            double result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class ToStringCasterExample {
        public static void main(String[] args) {
            Supplier<String> supplier = Suppliers.Caster.castToString(Suppliers.ChannelExtractors.doubleChannel("channel_double"));

            String result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class ToDoubleCasterExample {
        public static void main(String[] args) {
            Supplier<Double> supplier = Suppliers.Caster.castToDouble(Suppliers.ChannelExtractors.longChannel("channel_long"));

            double result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class LengthStringOperationsExample {
        public static void main(String[] args) {
            Supplier<Long> supplier = Suppliers.StringOperators.length(Suppliers.ChannelExtractors.stringChannel("channel_string"));

            long result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class ContainsStringOperationsExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.StringOperators.contains("test", Suppliers.ChannelExtractors.stringChannel("channel_string"));

            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }

    public static class MatchStringOperationsExample {
        public static void main(String[] args) {
            Supplier<Boolean> supplier = Suppliers.StringOperators.match("^test.*", Suppliers.ChannelExtractors.stringChannel("channel_string"));

            boolean result = supplier.extract(record);

            logger.info("supplier result: {}", result);
        }
    }
}
