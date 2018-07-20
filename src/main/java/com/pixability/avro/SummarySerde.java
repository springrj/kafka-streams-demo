package com.pixability.avro;

import com.pixability.model.LineItemPlatformSummaryDaily;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SummarySerde implements Serde<LineItemPlatformSummaryDaily> {

    private boolean isKey = false;
    private Map<String, ?> configs = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.configs = configs;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public Serializer<LineItemPlatformSummaryDaily> serializer() {
        SummarySerializer serializer = new SummarySerializer();
        serializer.configure(configs, isKey);
        return serializer;
    }

    @Override
    public Deserializer<LineItemPlatformSummaryDaily> deserializer() {
        SummaryDeserializer deserializer = new SummaryDeserializer();
        deserializer.configure(configs, isKey);
        return deserializer;
    }
}
