package com.mikhailkarpov.rabbitmq.publishsubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Publisher {

    private static final String EXCHANGE = "logs";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection()) {

            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE, "fanout");

            for (int i = 1; i <= 100; i++) {
                String message = "Logging message #" + i;
                channel.basicPublish(EXCHANGE, "", null, message.getBytes(UTF_8));
                System.out.println("Sent: " + message);

                Thread.sleep((long) (Math.random() * 1000));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
