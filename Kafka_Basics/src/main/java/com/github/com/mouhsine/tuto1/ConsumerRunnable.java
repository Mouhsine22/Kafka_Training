package com.github.com.mouhsine.tuto1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());


    public ConsumerRunnable(CountDownLatch latch, String topic , String bootstrapServers, String groupId){
        this.latch = latch;
        this.consumer = DefineConsumer(topic, defineProperties(bootstrapServers, groupId));
    }


    public Properties defineProperties(String bootstrapServers, String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public KafkaConsumer<String, String> DefineConsumer(String topic, Properties properties){
        // create consumer
        consumer = new KafkaConsumer<String, String>(properties);

        // subcribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    @Override
    public void run() {

        //poll for new data
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String, String> record : records){
                    logger.info("Recived new metadata: \n" +
                            "Key : " + record.key() + "\n" +
                            "Value : " + record.value() + "\n" +
                            "Partition : " + record.partition() + "\n" +
                            "offset : " + record.offset() + "\n" +
                            "Timestamp : " + record.timestamp() + "\n"
                    );
                }
            }
        }catch (WakeupException e){

            logger.info("Recevied shutdown signal !!");
        } finally {
            consumer.close();
            // tell our main code we're done with the consumer
            latch.countDown();
        }


    }

    public void shutdown(){
        consumer.wakeup();
    }
}