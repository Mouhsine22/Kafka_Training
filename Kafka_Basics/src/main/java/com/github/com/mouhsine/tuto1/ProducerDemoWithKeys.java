package com.github.com.mouhsine.tuto1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i=0 ; i < 10; i++) {
            // Create producer record

            String topic = "first_topic";
            String key = "id_"+ Integer.toString(i);
            String value = "hello world" + Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            //my key
            logger.info("my key :" +key);

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // excutes every time a record is sucss or exp
                    if (e == null) {
                        logger.info("Recived new metadata: \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing: ", e);
                    }

                }
            }).get();
        }

        producer.flush();
        producer.close();
    }
}
