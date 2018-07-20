package com.pixability;

import com.pixability.kafka.KafkaConfig;
import com.pixability.model.Key;
import com.pixability.model.LineItemPlatformSummaryDaily;
import com.pixability.model.SummaryKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.state.SessionStore;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String args[]) throws InterruptedException {
        KafkaConfig kafkaConfig = new KafkaConfig("line_item_platform_summary_daily");
        kafkaConfig.setNumStreams(1);
        kafkaConfig.setGroupId("rspring-test2");
        kafkaConfig.setBootstrapServers("127.0.0.1:9092");
        kafkaConfig.setZookeeperConnect("127.0.0.1:2181");

        kafkaConfig.setParams(new HashMap<String, Object>() {{
            put("application.id", "rspring-test");
            put("client.id", "rspring");
            put("consumer.group.id", "rspring-test");
            put("request.timeout.ms", "40000");
            put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LogAndSkipOnInvalidTimestamp.class.getName());
            put("schema.registry.url", "http://localhost:8081/");
            put("enable.auto.commit", true);
        }});

        String topicName = "line_item_platform_summary_daily";

        StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde<Key> keySerde = new SpecificAvroSerde<>();
        keySerde.configure(kafkaConfig.getParams(), true);

        SpecificAvroSerde<SummaryKey> summaryKeySerde = new SpecificAvroSerde<>();
        summaryKeySerde.configure(kafkaConfig.getParams(), true);

        SpecificAvroSerde<LineItemPlatformSummaryDaily> summarySerde = new SpecificAvroSerde<>();
        summarySerde.configure(kafkaConfig.getParams(), false);


        KStream<Key, LineItemPlatformSummaryDaily> stream = builder.stream(topicName,
                Consumed.with(keySerde, summarySerde));

        stream.groupBy((key, value) ->
                new SummaryKey(value.getDate(), value.getLineItemId(), value.getPlatform()), Serialized.with(summaryKeySerde, summarySerde))
                .windowedBy(SessionWindows.with(30000))
        .aggregate(
                LineItemPlatformSummaryDaily::new,
                // add
                (key, record, agg) -> {
                    agg.setLineItemId(record.getLineItemId());
                    agg.setInsertionOrderId(record.getInsertionOrderId());
                    agg.setPlatform(record.getPlatform());
                    agg.setDate(record.getDate());

                    long contractImpressions = agg.getContractImpressions() != null ? agg.getContractImpressions() : 0;
                    agg.setContractImpressions(contractImpressions + record.getContractImpressions());

                    long platformImpressions = agg.getPlatformImpressions() != null ? agg.getPlatformImpressions() : 0;
                    agg.setPlatformImpressions(platformImpressions + record.getPlatformImpressions());
                    return agg;
                },
                (aggKey, aggOne, aggTwo) -> {
                    System.out.println(aggTwo.toString());
                    return aggTwo;
                },
                Materialized.<SummaryKey, LineItemPlatformSummaryDaily, SessionStore<Bytes, byte[]>>as("aggregates_per_session2")
                        .withKeySerde(summaryKeySerde)
                        .withValueSerde(summarySerde))
        .toStream()
        .map((key, value) -> new KeyValue<>(key.key(), value))
        .to("aggregated_topic", Produced.with(summaryKeySerde, summarySerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig.toProperties());
        streams.start();

        Thread.sleep(60000 * 5);

        streams.close(60, TimeUnit.SECONDS);

        System.exit(0);
    }

}
