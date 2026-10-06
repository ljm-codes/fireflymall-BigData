package com.fm.Tools.SequenceBased.JSON;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.SearchRankData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JsonDeserializer implements DeserializationSchema<SearchRankData<CommodityRankData>> {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();

        // 配置LocalDateTime的序列化/反序列化格式（和你的JSON中时间格式匹配）
        // 如果你JSON中的searchTime是字符串（如"2026-01-13T15:03:56.656"），用这个格式：
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(formatter));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(formatter));

        // 注册模块（核心：启用Java 8时间类型支持）
        objectMapper.registerModule(javaTimeModule);
    }

    @Override
    public SearchRankData<CommodityRankData> deserialize(byte[] bytes) throws IOException {
        if(bytes==null||bytes.length==0){
            return null;
        }
        return objectMapper.readValue(bytes, new TypeReference<SearchRankData<CommodityRankData>>() {});
    }

    @Override
    public boolean isEndOfStream(SearchRankData<CommodityRankData> commodityRankDataSearchRankData) {
        return false;
    }

    @Override
    public TypeInformation<SearchRankData<CommodityRankData>> getProducedType() {
        return TypeInformation.of(new TypeHint<SearchRankData<CommodityRankData>>() {});
    }
}
