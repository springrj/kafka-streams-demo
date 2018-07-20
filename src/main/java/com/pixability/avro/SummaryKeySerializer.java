package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaSerializer;
import com.pixability.model.SummaryKey;

public class SummaryKeySerializer extends AvroKafkaSerializer<SummaryKey> {
    public SummaryKeySerializer() {
        super(SummaryKey.getClassSchema());
    }
}
