package main.systems.consumer;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class ExchangeReceiver {
    private static final String EXCHANGER_NAME = "direct_exchanger";

    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Enter channel to join[set_topic {channel}]:");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        String channelToJoin = reader.readLine();
        String[] tokens = channelToJoin.split(" ");

        toReceive(tokens[1]);
    }

    private static void toReceive(String channelToJoin) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        System.out.println("Started on queueName = " + queueName);

        channel.queueBind(queueName, EXCHANGER_NAME, channelToJoin);
        System.out.println("Waiting in channel = " + channel);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(Thread.currentThread().getName() + " Received message = " + message);
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
