package rabbitmq.demos.push_pull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

public class PushPullDemo {

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        //readMessagesWithPushModel(channel);
        readMessagesWithPullModel(channel);

        channel.close();
        conn.close();
	}
	
	private static void readMessagesWithPushModel(Channel channel) throws IOException
    {
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
	             System.out.println("Message: " + message);	             
	         }
	     };       

        String consumerTag = channel.basicConsume("my.queue1", true, consumer);

        System.out.print("Subscribed. Press enter to unsubscribe and exit.");
        Scanner reader = new Scanner(System.in);
        reader.nextLine();

        channel.basicCancel(consumerTag);
    }
	
	private static void readMessagesWithPullModel(Channel channel) throws IOException, InterruptedException
    {
		System.out.println("Reading messages from queue. Press enter to exit.");
		
		InputStreamReader reader = new InputStreamReader(System.in); 
		
        while(true)
        {
        	System.out.println("Trying to get a message from the queue...");

            GetResponse result = channel.basicGet("my.queue1", true);
            if(result != null)
            {
            	String message = new String(result.getBody());
            	System.out.println("Message: " + message);	  
            }
            
            if(reader.ready())
            	return;
            
            Thread.sleep(2000);
        }
    }
}
