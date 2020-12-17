package com.github.com.mouhsine.tuto1;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoThread {

    private ConsumerDemoThread() {

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my_third_app";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, topic, bootstrapServers, groupId);

        //start thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdowb hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("caught shutdown hook");
            ( (ConsumerRunnable) myConsumerRunnable ).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited !!!");

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }


    }

    public static void main(String[] args) {

        new ConsumerDemoThread().run();

    }


}
