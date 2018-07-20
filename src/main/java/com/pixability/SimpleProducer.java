package com.pixability;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.pixability.avro.KeySerializer;
import com.pixability.avro.SummarySerializer;
import com.pixability.model.Key;
import com.pixability.model.LineItemPlatformSummaryDaily;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        //Assign topicName to string variable
        String topicName = "line_item_platform_summary_daily";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", KeySerializer.class.getName());
        props.put("value.serializer", SummarySerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081/");

        Producer<Key, LineItemPlatformSummaryDaily> producer = new KafkaProducer<>(props);

        for(int i = 0; i < 1000; i++) {
            LineItemPlatformSummaryDaily record = generateRecord();
            Key key = new Key(record.getLineItemId());
            producer.send(new ProducerRecord<>(topicName, key, record));
            System.out.println("Message sent successfully");
            Thread.sleep(250);
        }
        producer.close();
    }

    static final List<String> PLATFORMS = new ArrayList<String>() {{
        add("AW");
        add("FB");
        add("TW");
    }};

    private static LineItemPlatformSummaryDaily generateRecord() {
        LineItemPlatformSummaryDaily record = new LineItemPlatformSummaryDaily();
        record.setLineItemId((long)(Math.random()*10+1));
        record.setInsertionOrderId(1L);
        record.setPlatform(PLATFORMS.get(new Random().nextInt(PLATFORMS.size())));
        record.setDate("2018-07-" + String.format("%02d", (int)(Math.random()*10+1)));
        record.setContractImpressions((long)(Math.random()*1000+1));
        record.setPlatformImpressions((long)(Math.random()*1000+1));
        return record;
    }

}
