package rabbitmq.demos.request_reply;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Replier {
	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        final Channel channel = conn.createChannel();
        
        Consumer consumer = new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {
	             String request = new String(body);
	             System.out.println("Request received:" + request);	 
	             String response = "Response for " + request;
	             channel.basicPublish("", "responses", null, response.getBytes());
	         }
	     };       

       channel.basicConsume("requests", true, consumer);

       System.out.println("Press enter to exit.");
       Scanner reader = new Scanner(System.in);
       reader.nextLine();

       channel.close();
       conn.close();
	}
}
