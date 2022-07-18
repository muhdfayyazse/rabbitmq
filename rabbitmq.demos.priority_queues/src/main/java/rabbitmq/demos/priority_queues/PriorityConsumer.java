package rabbitmq.demos.priority_queues;

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

public class PriorityConsumer {

	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        final Channel channel = conn.createChannel();

         //Don't send a new message to this worker 
         //until it has processed and acknowledged the last one
         channel.basicQos(0, 1, false);

         //Create consumers to receive messages
         Consumer consumer = new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {
	             long deliveryTag = envelope.getDeliveryTag();
	             
	             String message = new String(body);
	       
	             System.out.println("Processing message -> '"+ message + "' ...");
	             
	             try
	             {
	            	 Thread.sleep(1000);
	             }
	             catch(InterruptedException ex) {}
	             
	             System.out.println("FINISHED");
	             
	             channel.basicAck(deliveryTag, false);
	         }
	     };
         
        String consumerTag = channel.basicConsume("my.queue", false, consumer);  	     
        
        System.out.println("Subscribed to the queue. Waiting for messages. Press enter to exit.");
        System.in.read();

         //Unsubscribe
         channel.basicCancel(consumerTag);

         channel.close();
         conn.close();
	}
}
