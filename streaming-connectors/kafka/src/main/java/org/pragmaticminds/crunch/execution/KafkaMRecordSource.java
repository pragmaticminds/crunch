package org.pragmaticminds.crunch.execution;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.pragmaticminds.crunch.LoggingUtil;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.serialization.JsonDeserializerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * This class implements the {@link MRecordSource} for the use with a {@link KafkaConsumer}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 29.08.2018
 */
public class KafkaMRecordSource implements MRecordSource {

    static final long POLL_TIMEOUT_MS = 1_000L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaMRecordSource.class);

    private transient KafkaConsumer<String, UntypedValues>      consumer;
    private transient Iterator<ConsumerRecord<String, UntypedValues>> recordIterator;
    
    /**
     * Only for testing constructor. Gets a {@link KafkaConsumer} as parameter.
     * !!!The consumer must be set to autocommit!!!
     *
     * @param consumer provides this source with {@link UntypedValues}s.
     *
     * package private -> so it can only be used for testing
     */
    KafkaMRecordSource(KafkaConsumer<String, UntypedValues> consumer) {
        this.consumer = consumer;
    }
    
    /**
     * Main constructor. Creates a new instance of the {@link KafkaConsumer}
     * @param kafkaUrl to connect to kafka
     * @param kafkaGroup to connect to kafka
     * @param topics {@link List} of all to be subscribed
     */
    @SuppressWarnings("squid:S2095") // KafkaConsumer is responsible for the closing of the Deserializers
    public KafkaMRecordSource(String kafkaUrl, String kafkaGroup, Collection<String> topics){
        initialize(kafkaUrl, kafkaGroup, topics, null, false);
    }

    /**
     * Main constructor. Creates a new instance of the {@link KafkaConsumer}
     *
     * @param kafkaUrl      to connect to kafka
     * @param kafkaGroup    to connect to kafka
     * @param topics        {@link List} of all to be subscribed
     * @param fromBeginning If this should read from the beginning of the kafka topics if group is unset
     */
    @SuppressWarnings("squid:S2095") // KafkaConsumer is responsible for the closing of the Deserializers
    public KafkaMRecordSource(String kafkaUrl, String kafkaGroup, Collection<String> topics, boolean fromBeginning) {
        initialize(kafkaUrl, kafkaGroup, topics, null, fromBeginning);
    }

    /**
     * Main constructor. Creates a new instance of the {@link KafkaConsumer}
     * @param kafkaUrl to connect to kafka
     * @param kafkaGroup to connect to kafka
     * @param topics {@link List} of all to be subscribed
     * @param additionalProperties extra properties to be set up
     */
    public KafkaMRecordSource(String kafkaUrl, String kafkaGroup, Collection<String> topics,
                              Map<String, Object> additionalProperties) {
        initialize(kafkaUrl, kafkaGroup, topics, null, false);
    }
    
    /**
     * Helper for the constructors
     * @param kafkaUrl to connect to kafka
     * @param kafkaGroup to connect to kafka
     * @param topics {@link List} of all to be subscribed
     * @param additionalProperties extra properties to be set up
     */
    @SuppressWarnings("squid:S2095") // KafkaConsumer is responsible for the closing of the Deserializers
    private void initialize(
            String kafkaUrl, String kafkaGroup, Collection<String> topics, Map<String, Object> additionalProperties,
            boolean fromBeginning
    ) {
        Map<String, Object> properties;
        if(additionalProperties == null){
            properties = new HashMap<>();
        }else{
            properties = additionalProperties;
        }
        properties.put("bootstrap.servers", kafkaUrl);
        properties.put("group.id", kafkaGroup);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        if (fromBeginning) {
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        Deserializer<String> keyDeserializer = new JsonDeserializerWrapper<>(String.class);
        Deserializer<UntypedValues> valueDeserializer = new JsonDeserializerWrapper<>(UntypedValues.class);
        this.consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
        this.consumer.subscribe(topics);
    }
    
    /**
     * Request the next record.
     * Poll on {@link KafkaConsumer} with Long.MAX_VALUE as timeout.
     *
     * @return record from {@link KafkaConsumer}
     */
    @Override
    public UntypedValues get() {
        // polls as long as it get's records
        getMRecordsIfNoneAvailable();
    
        // return next record from the iterator
        ConsumerRecord<String, UntypedValues> next = recordIterator.next();
        if (logger.isTraceEnabled()) {
            if (next.offset() % LoggingUtil.getTraceLogReportCheckpoint() == 0) {
                logger.trace("Offset {} @ {}", next.offset(), Instant.ofEpochMilli(next.value().getTimestamp()));
            }
        }
        return next.value();
    }
    
    /**
     * polls until it gets records
     */
    private void getMRecordsIfNoneAvailable() {
        // while record iterator has no new records
        while(recordIterator == null || !recordIterator.hasNext()){
            ConsumerRecords<String, UntypedValues> consumerRecords = consumer.poll(POLL_TIMEOUT_MS);
            recordIterator = consumerRecords.iterator();
        }
    }
    
    /**
     * Check whether more records are available for fetch. In this case always true.
     *
     * @return true if records can be fetched using {@link #get()}. Always true, because Kafka is a stream and assumed
     * to have infinite records.
     */
    @Override
    public boolean hasRemaining() {
        return true;
    }
    
    /**
     * Is called before first call to {@link #hasRemaining()} or {@link #get()}.
     * In this case nothing is to do.
     */
    @Override
    public void init() { /* nothing to do in here */ }
    
    /**
     * Is called after processing has ended (either by cancelling or by exhausting the source).
     * Closes the consumer provided with the constructor.
     */
    @Override
    public void close() {
        consumer.close();
    }
    
    /**
     * Returns the Kind of the record source.
     *
     * @return Kind of the source. In this case {@link Kind}.INFINITE, because Kafka is a stream source and should be
     * infinite.
     */
    @Override
    public Kind getKind() {
        return Kind.INFINITE;
    }
}
