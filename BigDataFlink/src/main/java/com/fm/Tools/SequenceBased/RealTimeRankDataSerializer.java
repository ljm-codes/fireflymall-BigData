package com.fm.Tools.SequenceBased;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fm.ODS.RealTimeRankData;
import org.apache.kafka.common.serialization.Serializer;

public class RealTimeRankDataSerializer  implements Serializer<RealTimeRankData> {

    public static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(String topic, RealTimeRankData data) {
        if (data == null) {
            return null;
        }
        try{
            return objectMapper.writeValueAsString(data).getBytes();
        }catch (Exception e){
            throw new RuntimeException("序列化RealTimeRankData失败", e);
        }
    }
}
