package rabbitmq.demos.pubsub;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PubSubPublisher {

	public static void main(String[] args)  throws IOException, TimeoutException, InterruptedException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

      //Enable channel level publisher confirms (to be able to get confirmations from RabbitMQ broker)
        channel.confirmSelect();

        Scanner reader = new Scanner(System.in);
        
        while (true)
        {
        	System.out.print("Enter message:");
        	String message = reader.nextLine();

            if (message == "exit")
                break;

            channel.basicPublish("ex.fanout", "", null, message.getBytes());
        }

        //Wait until all published messages are confirmed
        channel.waitForConfirms();

        channel.close();
        conn.close();
	}

}
