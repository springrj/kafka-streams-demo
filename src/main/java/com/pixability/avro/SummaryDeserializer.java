package com.pixability.avro;

import com.pixability.kafka.avro.AvroKafkaDeserializer;
import com.pixability.model.LineItemPlatformSummaryDaily;


public class SummaryDeserializer extends AvroKafkaDeserializer<LineItemPlatformSummaryDaily> {
    public SummaryDeserializer() {
        super(LineItemPlatformSummaryDaily.getClassSchema());
        try {
//            this.schemaRegistry().register(subject("rspring-test-aggregated_line_item_platform_summary_daily-repartition"), LineItemPlatformSummaryDaily.getClassSchema());
//            this.schemaRegistry().register(subject("line_item_platform_summary_daily"), LineItemPlatformSummaryDaily.getClassSchema());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
