package rabbitmq.demos.defaultDemo;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DefaultDemo {

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

            channel.basicPublish(
                "",
                "my.queue1",
                null,
                "Message with routing key my.queue1".getBytes());

            channel.basicPublish(
                "",
                "my.queue2",
                null,
                "Message with routing key my.queue2".getBytes());

            //Wait until all published messages are confirmed
            channel.waitForConfirms();

	}

}
