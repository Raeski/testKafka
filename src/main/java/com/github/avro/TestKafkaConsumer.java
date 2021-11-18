package com.github.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import uol.pagseguro.servicedesk.salesforce.routeactivity.RouteActivityRecord;

import java.util.Arrays;
import java.util.Properties;


public class TestKafkaConsumer {


    public void execute() throws InterruptedException {
        KafkaConsumer<String, RouteActivityRecord> consumer = createConsumer();
        consumer.subscribe(Arrays.asList("client-avro"));
        processRecords(consumer);
    }
    private static KafkaConsumer<String, RouteActivityRecord> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RouteActivityRecord");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());  //<----------------------

        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        return new KafkaConsumer<>(props);
    }
    private void processRecords(KafkaConsumer<String, RouteActivityRecord> consumer) throws InterruptedException {
        while (true) {
            ConsumerRecords<String, RouteActivityRecord> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, RouteActivityRecord> record : records) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(),                                             record.key(), record.value());
                lastOffset = record.offset();
            }
            System.out.println("lastOffset read: " + lastOffset);
            process();
        }
    }
    private static void process() throws InterruptedException {
        Thread.sleep(20);
    }

}
