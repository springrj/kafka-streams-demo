package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaSerializer;
import com.pixability.model.Key;

public class KeySerializer extends AvroKafkaSerializer<Key> {
    public KeySerializer() {
        super(Key.getClassSchema());
    }
}
