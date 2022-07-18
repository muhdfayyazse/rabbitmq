package rabbitmq.demos.alternate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class AlternateExchange {

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		
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

        channel.exchangeDeclare(
            "ex.fanout",
            BuiltinExchangeType.FANOUT,
            true,
            false,
            null);
        
        Map<String, Object> excArgs = new HashMap<String, Object>();
        excArgs.put("alternate-exchange","ex.fanout");
        
        channel.exchangeDeclare(
                "ex.direct",
                BuiltinExchangeType.DIRECT,
                true,
                false,
                excArgs);
        
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

            channel.queueDeclare(
                "my.unrouted",
                true,
                false,
                false,
                null);

            channel.queueBind("my.queue1", "ex.direct", "video");
            channel.queueBind("my.queue2", "ex.direct", "image");
            channel.queueBind("my.unrouted", "ex.fanout", "");

            channel.basicPublish(
                    "ex.direct",
                    "video",
                    null,
                    "Message with routing key video".getBytes());

                channel.basicPublish(
                    "ex.direct",
                    "text",
                    null,
                    "Message with routing key text".getBytes());
                
                //Wait until all published messages are confirmed
                channel.waitForConfirms();
	}
}
