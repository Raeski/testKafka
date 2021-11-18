package com.github.avro;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uol.pagseguro.servicedesk.salesforce.routeactivity.RouteActivityRecord;

import java.util.Properties;

public class KafkaAvroProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());

        Producer<String, RouteActivityRecord> producer = new KafkaProducer<>(properties);

        String topic = "client-avro";

        RouteActivityRecord lead = RouteActivityRecord.newBuilder()
                .setTrademark("Compass uol")
                .setAddressComplement("rua teste teste")
                .setAddressName("rua teste teste")
                .setAddressNumber("102")
                .setPostalCode("109474")
                .setIsPagsClient("0")
                .setCity("asdas")
                .setPhone("429144021")
                .setDocument("1231234")
                .setPersonType("test")
                .setCustomerOrigin("tes")
                .setState("teste")
                .setDistrict("test")
                .setRouteActivityNumber(123l)
                .setRouteNumber("asd")
                .setLatitude("123")
                .setLongitude("123")
                .setMcc("123")
                .setCnae("1234123")
                .setNameContact("Contato teste")
                .setPresumedRevenue(123.1)
                .setResponsibleName("test")
                .setSegment("tst")
                .setClientSituation("test")
                .setAlertType("test")
                .setBiggestRevenue(123.1)
                .setBiggestRevenueDate("1233.1")
                .setPresumedRevenue(123.0)
                .setPresumedMonthlyTpv(123.0)
                .setPortfolioCode(12312312l)
                .build();

        ProducerRecord<String, RouteActivityRecord> producerRecord = new ProducerRecord<>(
                topic, lead
        );

        System.out.println(lead);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();



    }
}
