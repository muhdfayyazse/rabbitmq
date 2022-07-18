package rabbitmq.demos.work_queues;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class WorkQueuesDemo {

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		
		System.out.print("Enter the name for this worker:");
		Scanner reader = new Scanner(System.in);
		final String workerName = reader.nextLine();
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        
        Consumer consumer = new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {
	             String message = new String(body);	             
	             System.out.println("["+ workerName +"] Message:" + message);	             
	         }
	     };       

	     String consumerTag = channel.basicConsume("my.queue1", true, consumer);

	     System.out.println("Waiting for messages. Press enter to exit.");
	     reader.nextLine();

         channel.close();
         conn.close();
	}

}
