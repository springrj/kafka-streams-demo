package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaDeserializer;
import com.pixability.model.Key;


public class KeyDeserializer extends AvroKafkaDeserializer<Key> {
    public KeyDeserializer() {
        super(Key.getClassSchema());
    }

    @Override
    public Key deserialize(String topic, byte[] bytes) {
        return super.deserialize(topic, bytes);
    }
}