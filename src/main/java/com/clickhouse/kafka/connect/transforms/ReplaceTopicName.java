package com.clickhouse.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@SuppressWarnings("unused")
public class ReplaceTopicName<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceTopicName.class);
    @SuppressWarnings("unused")
    public static final String OVERVIEW_DOC = "Update the record topic using the configured topicName.";

    private interface ConfigName {
        String TOPIC_FIELD = "topicName";
    }

    private String topicName;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH,
                    "New topic name.");

    @Override
    public R apply(R record) {
        return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
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

        LOGGER.info("Configuring ReplaceTopicName, topicName: {}", topicName);
    }
}
