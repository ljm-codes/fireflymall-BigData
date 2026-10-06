package com.fm.Tools.SendToExternal;

import lombok.Getter;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Properties;

/**
 * Doris工具类，用于将数据发送到Doris数据库
 * <p>仅支持JSON和CSV格式</p>
 * <p>数据必须要经过toString()方法转换为字符串</p>
 */
@Getter
public class SendToDorisTool {

    private static final String DORIS_FENODES = "node1:8030";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";
    private final String tableIdentifier;
    private final String labelPrefix;
    private boolean isBatchMode = false; // 是否攒批写入模式
    private Integer batchSize = 1000; // 批量写入大小
    private Integer BatchIntervalMs = 1000; // 批量写入时间间隔，单位毫秒
    private Integer maxRetries = 3; // 最大重试次数
    private Integer bufferFlushMaxBytes = 5 * 1024 * 1024; // 批量写入最大字节数 (默认5MB)
    private Integer bufferCount = 3; // 缓冲区数量（默认3）

    public enum DATATYPE {
        JSON,
        CSV
    }

    private final DATATYPE dataType;

    /**
     * 默认构造函数，数据类型默认JSON
     */
    public SendToDorisTool() {
        this("FireflyMall.idTopN", DATATYPE.JSON, "FireflyMall-");
    }

    public SendToDorisTool(String tableIdentifier, DATATYPE dataType, String labelPrefix, Integer batchSize, Integer BatchIntervalMs, Integer maxRetries, Integer bufferFlushMaxBytes, Integer bufferCount) {

        this(tableIdentifier, dataType, labelPrefix);

        this.isBatchMode = true;

        if (batchSize <= 0 || BatchIntervalMs <= 0 || maxRetries <= 0) {
            throw new IllegalArgumentException("batchSize, BatchIntervalMs, maxRetries must be greater than 0");
        }

        if (bufferFlushMaxBytes <= 0 || bufferCount <= 0) {
            throw new IllegalArgumentException("bufferFlushMaxBytes, bufferCount must be greater than 0");
        }

        this.batchSize = batchSize;
        this.BatchIntervalMs = BatchIntervalMs;
        this.maxRetries = maxRetries;
        this.bufferFlushMaxBytes = bufferFlushMaxBytes;
        this.bufferCount = bufferCount;


    }

    /**
     * 可自定义库名.表名和数据类型
     *
     * @param tableIdentifier 库名.表名
     * @param dataType        数据类型
     */
    public SendToDorisTool(String tableIdentifier, DATATYPE dataType, String labelPrefix) {

        if (tableIdentifier == null) {
            throw new IllegalArgumentException("tableIdentifier cannot be null");
        }

        if (labelPrefix == null) {
            throw new IllegalArgumentException("labelPrefix cannot be null");
        }

        if (dataType == null) {
            throw new IllegalArgumentException("dataType cannot be null");
        }

        String[] tableIdentifierArray = tableIdentifier.split("\\.");
        if (tableIdentifierArray.length != 2) {
            throw new IllegalArgumentException("tableIdentifier must be in the format of database.table");
        }

        this.dataType = dataType;
        this.tableIdentifier = tableIdentifier;
        this.labelPrefix = labelPrefix;

    }

    private DorisSink.Builder<String> initMainDorisOptions() {

        DorisSink.Builder<String> builder = DorisSink.builder();

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(DORIS_FENODES)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .setTableIdentifier(tableIdentifier)
                .build();

        Properties prop = sendDorisPropByDataType(dataType);

        DorisExecutionOptions.Builder executionOptionsBuilder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setDeletable(false)
                .setStreamLoadProp(prop);

        if (isBatchMode) {
            executionOptionsBuilder
                    .setBatchMode(true) // 开启批量模式
                    .setBufferFlushMaxRows(batchSize) // 按行数触发刷写
                    .setBufferFlushIntervalMs(BatchIntervalMs) //  按时间间隔触发刷写
                    .setMaxRetries(maxRetries) // 最大重试次数
                    .setBufferFlushMaxBytes(bufferFlushMaxBytes) // 按字节数触发刷写
                    .setBufferSize(bufferFlushMaxBytes) // 缓冲区大小，与最大刷写字节数保持一致
                    .setBufferCount(bufferCount);
        }

        DorisExecutionOptions executionOptions = executionOptionsBuilder.build();

        builder.setDorisExecutionOptions(executionOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

        return builder;
    }

    public void sendDataToDoris(DataStream<String> data, int sinkParallelism) {
        DorisSink.Builder<String> dorisSink = initMainDorisOptions();

        data.sinkTo(dorisSink.build())
                .setParallelism(sinkParallelism);
    }

    private Properties sendDorisPropByDataType(DATATYPE dataType) {
        Properties prop = new Properties();

        switch (dataType) {
            case JSON:
                prop.setProperty("read_json_by_line", "true");
                prop.setProperty("format", "json");
                break;
            case CSV:
                prop.setProperty("format", "csv");
                prop.setProperty("column_separator", ",");
                break;
            default:
                throw new IllegalArgumentException("dataType must be JSON or CSV");
        }
        //  忽略未知字段
        prop.setProperty("json-ignore-unknown-fields", "true");

        return prop;
    }



}
