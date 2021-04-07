package com.mikhailkarpov.rabbitmq.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN;

public class TaskSender {

    private static final String QUEUE_NAME = "task_queue";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection()) {

            Channel channel = connection.createChannel();
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            for (int i = 1; i <= 100; i++) {
                String task = "Task #" + i;
                channel.basicPublish("", QUEUE_NAME, PERSISTENT_TEXT_PLAIN, task.getBytes(StandardCharsets.UTF_8));
                System.out.println("Sent task: " + task);

                Thread.sleep((long) (Math.random() * 1000));
            }

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
