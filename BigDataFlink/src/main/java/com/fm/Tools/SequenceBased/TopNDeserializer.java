package com.fm.Tools.SequenceBased;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fm.ADS.TopNData;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class TopNDeserializer implements Deserializer<TopNData> {

    private final static ObjectMapper mapper = new ObjectMapper();

    @Override
    public TopNData deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;
            else
                return mapper.readValue(new String(data, StandardCharsets.UTF_8), TopNData.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to TopNData");
        }
    }
}
