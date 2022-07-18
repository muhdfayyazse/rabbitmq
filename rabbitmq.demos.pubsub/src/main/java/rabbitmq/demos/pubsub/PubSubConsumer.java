package rabbitmq.demos.pubsub;

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

public class PubSubConsumer {

	public static void main(String[] args) throws IOException, TimeoutException {
		Scanner reader = new Scanner(System.in);
		 
		System.out.print("Enter the queue name:");
        final String queueName = reader.nextLine();

        ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        
      //Create consumers to receive messages
        Consumer consumer = new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {
	             String message = new String(body);
	             System.out.println("Subscriber [" + queueName + "] Message: " + message);	             
	         }
	     };       

        String consumerTag = channel.basicConsume(queueName, true, consumer);

        System.out.print("Subscribed to the queue '"+queueName+"'. Press enter to exit.");
        reader.nextLine();

        channel.close();
        conn.close();    
	}
}
