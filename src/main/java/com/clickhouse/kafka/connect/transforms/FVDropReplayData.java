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

import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

@SuppressWarnings("unused")
public class FVDropReplayData<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FVDropReplayData.class);
    @SuppressWarnings("unused")
    public static final String OVERVIEW_DOC = "Remove the binary payloads from events that are replay-events";

    private static final String PURPOSE = "transform payload";

    private interface ConfigName {
        String TOPIC_FIELD = "topicName";
    }

    private String topicName;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FVDropReplayData.ConfigName.TOPIC_FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH,
                    "Source topic name");

    @Override
    public R apply(R record) {

        try {
            final Struct value = requireStruct(record.value(), PURPOSE);
            final Schema valueSchema = record.valueSchema();

            Field is_replay_event_field = value.schema().field("is_replay_event");
            if (is_replay_event_field != null) {
                Object is_replay_event_value = value.get(is_replay_event_field);
                if (is_replay_event_value instanceof Boolean && Boolean.FALSE.equals(is_replay_event_value)) {
                    value.put("payload", "");
                }
            }

            return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, value, record.timestamp());
        } catch (Exception e) {
            LOGGER.error("Error removing binary payload", e);
            throw e;
        }
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

        LOGGER.info("Configuring FVDropReplayData, topicName: {}", topicName);
    }
}
