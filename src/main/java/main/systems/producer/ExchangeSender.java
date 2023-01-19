package main.systems.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class ExchangeSender {
    private static final String EXCHANGER_NAME = "direct_exchanger";

    public static void main(String[] args) throws IOException {
        System.out.println("Enter string to send[first arg is theme]:");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        String message = reader.readLine();

        toSend(message);
    }

    private static void toSend(String message) {
        String[] tokens = message.split(" ");
        String messageToSend = tokens[1]+" "+tokens[2];
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);


            channel.basicPublish(EXCHANGER_NAME, tokens[0], null, messageToSend.getBytes());
            System.out.println("Sent message = " + messageToSend);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
