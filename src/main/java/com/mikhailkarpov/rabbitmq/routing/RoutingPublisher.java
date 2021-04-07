package com.mikhailkarpov.rabbitmq.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RoutingPublisher {

    private static final String EXCHANGE = "routing_logs";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection()) {

            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE, "direct");

            Random random = new Random();

            for (int i = 1; i <= 100; i++) {
                String message;
                int evenOrOdd = random.nextInt(2) + 1;

                if (evenOrOdd % 2 == 0) {
                    message = "Info message #" + i;
                    channel.basicPublish(EXCHANGE, "info", null, message.getBytes(UTF_8));
                } else {
                    message = "Error message #" + i;
                    channel.basicPublish(EXCHANGE, "error", null, message.getBytes(UTF_8));
                }
                System.out.println("Sent: " + message);

                Thread.sleep((long) (Math.random() * 1000));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
