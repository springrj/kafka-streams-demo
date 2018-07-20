package com.pixability.avro;

import com.pixability.model.Key;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KeySerde implements Serde<Key> {

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
    public Serializer<Key> serializer() {
        KeySerializer serializer = new KeySerializer();
        serializer.configure(configs, isKey);
        return serializer;
    }

    @Override
    public Deserializer<Key> deserializer() {
        KeyDeserializer deserializer = new KeyDeserializer();
        deserializer.configure(configs, isKey);
        return deserializer;
    }
}
