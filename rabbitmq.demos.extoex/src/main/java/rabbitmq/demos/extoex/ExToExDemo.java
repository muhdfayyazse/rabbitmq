package rabbitmq.demos.extoex;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ExToExDemo {

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
                "exchange1",
                BuiltinExchangeType.DIRECT,
                true,
                false,
                null);
        
        channel.exchangeDeclare(
                "exchange2",
                BuiltinExchangeType.DIRECT,
                true,
                false,
                null);
        
        channel.queueDeclare(
                "queue1",
                true,
                false,
                false,
                null);

            channel.queueDeclare(
                "queue2",
                true,
                false,
                false,
                null);

            channel.queueBind("queue1", "exchange1", "key1");
            channel.queueBind("queue2", "exchange2", "key2");

            channel.exchangeBind("exchange2", "exchange1", "key2");
            
            channel.basicPublish(
                    "exchange1",
                    "key1",
                    null,
                    "Message with routing key key1".getBytes());

                channel.basicPublish(
                    "exchange1",
                    "key2",
                    null,
                    "Message with routing key key2".getBytes());

                //Wait until all published messages are confirmed
                channel.waitForConfirms();
	}

}
