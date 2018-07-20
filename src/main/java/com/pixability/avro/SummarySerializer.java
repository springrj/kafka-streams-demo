package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaSerializer;
import com.pixability.model.LineItemPlatformSummaryDaily;

public class SummarySerializer extends AvroKafkaSerializer<LineItemPlatformSummaryDaily> {
    public SummarySerializer() {
        super(LineItemPlatformSummaryDaily.getClassSchema());
    }
}
