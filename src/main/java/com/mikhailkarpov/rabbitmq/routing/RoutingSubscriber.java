package com.mikhailkarpov.rabbitmq.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RoutingSubscriber {

    private static final String EXCHANGE = "routing_logs";

    public static void main(String[] args) throws IOException, TimeoutException {

        if (args.length < 1) {
            System.err.println("Usage: Subscriber [info] [error]");
            System.exit(1);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE, "direct");
        String queue = channel.queueDeclare().getQueue();

        for (String severity : args) {
            channel.queueBind(queue, EXCHANGE, severity);
            System.out.println(String.format("Waiting for \"%s\" messages", severity));
        }

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received: " + message);
        };

        channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
        });

    }
}
