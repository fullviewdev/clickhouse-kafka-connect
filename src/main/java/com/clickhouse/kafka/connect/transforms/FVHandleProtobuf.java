package com.clickhouse.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

@SuppressWarnings("unused")
public class FVHandleProtobuf<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FVHandleProtobuf.class);
    @SuppressWarnings("unused")
    public static final String OVERVIEW_DOC = "Update the record topic using the configured topicName and handles binaryPayload.";

    private static final String PURPOSE = "transform payload";

    private interface ConfigName {
        String TOPIC_FIELD = "topicName";
    }

    private String topicName;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH,
                    "New topic name.");

    @Override
    public R apply(R record) {

        try {

            final Struct value = requireStruct(record.value(), PURPOSE);
            final Schema valueSchema = record.valueSchema();
            final Struct updatedValue = new Struct(valueSchema);

            // copy all the fields
            for (Field field : value.schema().fields()) {
                Object fieldValue = value.get(field);

                if (fieldValue != null && Objects.equals(field.name(), "binaryPayload") && fieldValue instanceof ByteBuffer) {
                    ByteBuffer baValue = (ByteBuffer) fieldValue;
                    final String payloadValue = new String(baValue.array(), StandardCharsets.UTF_8);

                    updatedValue.put("payload", payloadValue);
                } else {
                    updatedValue.put(field.name(), fieldValue);
                }
            }
            return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, updatedValue, record.timestamp());
        } catch (Exception e) {
            LOGGER.error("Error handling binary payload", e);
            throw e;
        }
//        return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.topicName = config.getString(ConfigName.TOPIC_FIELD);

        LOGGER.info("Configuring FVHandleProtobuf, topicName: {}", topicName);
    }
}
