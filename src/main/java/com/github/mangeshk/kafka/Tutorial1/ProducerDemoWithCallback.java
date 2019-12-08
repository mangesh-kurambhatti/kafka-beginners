package com.github.mangeshk.kafka.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers="127.0.0.1:9092";
        //create producer properties
        Properties  properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer

        KafkaProducer<String ,String>  producer=new KafkaProducer<String, String>(properties);

        //create producer record

        ProducerRecord<String,String> record = new ProducerRecord<String, String>("second_topic","Hello using Intellij");

        //send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully sent or an exceptiojn is thrown

                if(e == null)
                {
                    logger.info("Received new MetaData.\n"+
                            "Topi:c");
                }
                else
                {}
            }

        });

        producer.flush();
        producer.close();
    }

}
