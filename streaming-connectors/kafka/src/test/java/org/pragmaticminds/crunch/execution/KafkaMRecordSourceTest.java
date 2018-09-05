package org.pragmaticminds.crunch.execution;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.pragmaticminds.crunch.execution.KafkaMRecordSource.POLL_TIMEOUT_MS;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 29.08.2018
 */
public class KafkaMRecordSourceTest implements Serializable {
    
    private transient KafkaMRecordSource                   source;
    private transient KafkaConsumer<String, UntypedValues> consumer;
    
    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        UntypedValues record = Mockito.mock(UntypedValues.class);
        
        ConsumerRecord<String, UntypedValues> consumerRecord = new ConsumerRecord<>("testTopic", 1, 0L, "test1", record);
        
        List<ConsumerRecord<String, UntypedValues>> recordList = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            recordList.add(consumerRecord);
        }
        
        Map<TopicPartition, List<ConsumerRecord<String, UntypedValues>>> recordMap = new HashMap<>();
        recordMap.put(new TopicPartition("testTopic", 1), recordList);
        
        ConsumerRecords<String, UntypedValues> consumerRecords = new ConsumerRecords<>(recordMap);
        
        consumer = Mockito.mock(KafkaConsumer.class);
        Mockito.when(consumer.poll(POLL_TIMEOUT_MS)).thenReturn(consumerRecords);
        
        source = new KafkaMRecordSource(consumer);
    }
    
    @Test
    public void get() {
        // get MRecord 12 times, so that the consumer.poll method is called twice
        for (int i = 0; i < 12; i++) {
            UntypedValues mRecord = source.get();
            assertNotNull(mRecord);
        }
        
        // since there are 6 records in the consumerRecords, poll should only be called twice
        Mockito.verify(consumer, Mockito.times(2)).poll(POLL_TIMEOUT_MS);
    }
    
    @Test
    public void hasRemaining() {
        assertTrue(source.hasRemaining());
    }
    
    @Test
    public void init() {
        try{
            source.init();
        } catch (Exception ex){
            fail("failed to init");
        }
    }
    
    @Test
    public void close() {
        try{
            source.close();
        } catch (Exception ex){
            fail("failed to close consumer");
        }
    }
    
    @Test
    public void getKind() {
        assertEquals(MRecordSource.Kind.INFINITE, source.getKind());
    }
}