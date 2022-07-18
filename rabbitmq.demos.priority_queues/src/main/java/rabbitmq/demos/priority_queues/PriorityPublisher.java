package rabbitmq.demos.priority_queues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PriorityPublisher {

	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        
      //Create a "fanout" exchange
        channel.exchangeDeclare(
           "ex.fanout",
           BuiltinExchangeType.FANOUT,
           true,
           false,
           null);

        //Create a queue that supports message priorities (with max message priority = 2)
        Map<String, Object> argsQueue1 = new HashMap<String, Object>();
        argsQueue1.put("x-max-priority",2);

        channel.queueDeclare(
            "my.queue",
            true,
            false,
            false,
            argsQueue1);

        //Bind the queue to the fanout exchange
        channel.queueBind("my.queue", "ex.fanout", "");       
        
        Scanner reader = new Scanner(System.in);
        
        System.out.println("Publisher is ready. Press enter to start sending messages.");
        reader.nextLine();

        //Send sample messages with low (priority=1) and high (priority=2) priorities

        //Low priority messages
        sendMessage(channel, 1);
        sendMessage(channel, 1);
        sendMessage(channel, 1);

        //High priority messages
        sendMessage(channel, 2);
        sendMessage(channel, 2);
        
        System.out.println("Press enter to exit.");
        reader.nextLine();

        channel.queueDelete("my.queue");
        channel.exchangeDelete("ex.fanout");

        channel.close();
        conn.close();
	}
	
	private static void sendMessage(Channel channel, int priority) throws IOException
    {
        AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
        propBuilder.priority(priority);

        AMQP.BasicProperties propsMsg = propBuilder.build();

        String message = "Message with priority=" + priority;
        channel.basicPublish("ex.fanout", "", propsMsg, message.getBytes());

        System.out.println("SENT:"  + message);
    }
}
