package rabbitmq.demos.fanout;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class FanoutConsumer {

	public static void main(String[] args) throws IOException, TimeoutException {
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
	             long deliveryTag = envelope.getDeliveryTag();
	             
	             String message = new String(body);
	       
	             System.out.println("Message:" + message);
	             
	             channel.basicAck(deliveryTag, false);
	         }
	     };
        
	     String consumerTag = channel.basicConsume("my.queue1", false, consumer);
        
        System.out.println("Waiting for messages. Press enter to exit.");
        System.in.read();

	}

}
