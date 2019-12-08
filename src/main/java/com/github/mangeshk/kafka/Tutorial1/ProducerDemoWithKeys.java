package com.github.mangeshk.kafka.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrapServers="127.0.0.1:9092";
        //create producer properties
        Properties  properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer

        KafkaProducer<String ,String>  producer=new KafkaProducer<String, String>(properties);

        //create producer record

        for(int i=0;i<10;i++) {

            String topic="second_topic";
            String value="Hello World "+Integer.toString(i);
            String key="id_"+Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);//11075763

            logger.info("Key: "+key);
            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exceptiojn is thrown

                    if (e == null) {
                        logger.info("Received new MetaData.\n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing :", e);
                    }
                }

            }).get();
        }
        producer.flush();
        producer.close();
    }

}
