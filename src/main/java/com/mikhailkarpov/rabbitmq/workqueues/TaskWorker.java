package com.mikhailkarpov.rabbitmq.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TaskWorker {

    private static final String QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        channel.basicQos(1);

        System.out.println("Waiting for messages...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String task = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received task: " + task);

            doWork(task);

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
        });
    }

    private static void doWork(String task) {
        try {
            Thread.sleep((long) (Math.random() * 1000));
            System.out.println("Done: " + task);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
