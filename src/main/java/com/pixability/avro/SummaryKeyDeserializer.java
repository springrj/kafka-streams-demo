package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaDeserializer;
import com.pixability.model.SummaryKey;

public class SummaryKeyDeserializer extends AvroKafkaDeserializer<SummaryKey> {
    public SummaryKeyDeserializer() {
        super(SummaryKey.getClassSchema());
        try {
//            this.schemaRegistry().register(subject("rspring-test-aggregated_line_item_platform_summary_daily-repartition"), SummaryKey.getClassSchema());
//            this.schemaRegistry().register(subject("line_item_platform_summary_daily"), SummaryKey.getClassSchema());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}