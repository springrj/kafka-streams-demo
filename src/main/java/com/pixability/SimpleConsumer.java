package com.pixability;

import com.pixability.avro.*;
import com.pixability.model.LineItemPlatformSummaryDaily;
import com.pixability.model.SummaryKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) throws Exception{

        //Assign topicName to string variable
        String topicName = "aggregated_topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rspring-test2");
        props.put("acks", "all");
        props.put("key.deserializer", SummaryKeyDeserializer.class.getName());
        props.put("value.deserializer", SummaryDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081/");

        final Consumer<SummaryKey, LineItemPlatformSummaryDaily> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topicName));

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<SummaryKey, LineItemPlatformSummaryDaily> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                if (record.value() != null)
                    System.out.printf("Consumer Key:(%s, %d, %s) contract_impressions: %d\n",
                            record.key().getDate(), record.key().getLineItemId(),
                            record.key().getPlatform(), record.value().getContractImpressions());
            });

            consumer.commitAsync();
        }
        consumer.close();

        System.exit(0);
    }
}
