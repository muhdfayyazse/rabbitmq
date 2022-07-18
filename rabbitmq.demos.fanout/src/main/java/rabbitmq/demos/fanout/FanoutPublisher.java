package rabbitmq.demos.fanout;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class FanoutPublisher {

	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(
            "ex.fanout",
            BuiltinExchangeType.FANOUT,
            true,
            false,
            null);

        channel.queueDeclare(
            "my.queue1",
            true,
            false,
            false,
            null);

        channel.queueDeclare(
            "my.queue2",
            true,
            false,
            false,
            null);

        channel.queueBind("my.queue1", "ex.fanout", "");
        channel.queueBind("my.queue2", "ex.fanout", "");

        channel.basicPublish(
            "ex.fanout",
            "",
            null,
            "Message 1".getBytes()
            );

        channel.basicPublish(
            "ex.fanout",
            "",
            null,
            "Message 2".getBytes()
            );

        System.out.println("Press enter to exit.");
        System.in.read();
        
        channel.queueDelete("my.queue1");
        channel.queueDelete("my.queue2");
        channel.exchangeDelete("ex.fanout");

        channel.close();
        conn.close();
	}
}
