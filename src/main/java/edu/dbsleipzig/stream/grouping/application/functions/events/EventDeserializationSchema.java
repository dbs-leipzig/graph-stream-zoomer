package edu.dbsleipzig.stream.grouping.application.functions.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class EventDeserializationSchema extends AbstractDeserializationSchema<CDCEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    /**
     * For performance reasons it's better to create on ObjectMapper in this open method rather than
     * creating a new ObjectMapper for every record.
     */
    @Override
    public void open(InitializationContext context) {
        // JavaTimeModule is needed for Java 8 data time (Instant) support
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    /**
     * If our deserialize method needed access to the information in the Kafka headers of a
     * KafkaConsumerRecord, we would have implemented a KafkaRecordDeserializationSchema instead of
     * extending AbstractDeserializationSchema.
     */
    @Override
    public CDCEvent deserialize(byte[] message) throws IOException {
        byte[] message2 = Arrays.copyOfRange(message, 5, message.length);
        return objectMapper.readValue(message2, CDCEvent.class);
    }
}
