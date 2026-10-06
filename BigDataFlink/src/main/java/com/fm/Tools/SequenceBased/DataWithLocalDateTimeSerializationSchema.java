package com.fm.Tools.SequenceBased;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class DataWithLocalDateTimeSerializationSchema<T> implements SerializationSchema<T> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context){
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(T t) {
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize RecommendationSystemGenuineData", e);
        }
    }

}
