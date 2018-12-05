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

package org.pragmaticminds.crunch.execution;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.serialization.JsonDeserializerWrapper;
import org.pragmaticminds.crunch.serialization.JsonSerializerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Manually communication test with a real Kafka instance.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.09.2018
 */
public class KafkaMRecordSourceMT {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMRecordSourceMT.class);
    private final String        topic  = "KafkaMRecordSourceMT4";


    private KafkaMRecordSource source;
    private String             url    = "http://192.168.167.198:9092";
    private String             group  = UUID.randomUUID().toString(); // guaranty always individual group
    private List<String>       topics = new ArrayList<>();

    @Before
    public void setUp() {
        topics.add(topic);
    }

    @Test
    public void communicateWithSource(){
        saveSomeMRecordsToKafka();

        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        source = new KafkaMRecordSource(url, group, topics, additionalProperties);
        source.init();

        MRecordSource.Kind kind = source.getKind();
        assertTrue(source.hasRemaining());
        for (int i = 0; i < 100; i++) {
            UntypedValues record = source.get();
            if(record != null){
                logger.debug(String.format("record: %s", record.toString()));
            }else{
                logger.error("record: null");
            }
            assertNotNull(record);
        }
        source.close();
    }

    private void saveSomeMRecordsToKafka(){
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "http://192.168.167.198:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        Serializer<String> keySerializer = new JsonSerializerWrapper<>();
        Serializer<UntypedValues> valueSerializer = new JsonSerializerWrapper<>();
        KafkaProducer<String, UntypedValues> producer = new KafkaProducer<>(properties, keySerializer, valueSerializer);
        for (int i = 0; i < 100; i++) {
            UntypedValues record = createRecord(i);
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), record));
        }
        producer.close();
    }

    private UntypedValues createRecord(int i) {
        Map<String, Object> values = new HashMap<>();
        values.put("test", i);
        return UntypedValues.builder()
                .source(String.format("test%d", i))
                .prefix(" ")
                .timestamp(System.currentTimeMillis())
                .values(values)
                .build();
    }

    @Test
    public void trySerializeAndDeserialize(){
        JsonSerializerWrapper<UntypedValues> serializer = new JsonSerializerWrapper<>();
        JsonDeserializerWrapper<UntypedValues> deserializer = new JsonDeserializerWrapper<>(UntypedValues.class);
        UntypedValues record = createRecord(1);

        byte[] bytes = serializer.serialize("test", record);
        String message = new String(bytes);
        logger.debug("message: {}", message);
        UntypedValues result = deserializer.deserialize("test", bytes);
        assertEquals(record, result);
    }
}