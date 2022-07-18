package rabbitmq.demos.direct;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DirectDemo {

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
                "ex.direct",
                BuiltinExchangeType.DIRECT,
                true,
                false,
                null);
        
        channel.queueDeclare(
                "my.infos",
                true,
                false,
                false,
                null);

            channel.queueDeclare(
                "my.warnings",
                true,
                false,
                false,
                null);

            channel.queueDeclare(
                "my.errors",
                true,
                false,
                false,
                null);

            channel.queueBind("my.infos", "ex.direct", "info");
            channel.queueBind("my.warnings", "ex.direct", "warning");
            channel.queueBind("my.errors", "ex.direct", "error");
            
            channel.basicPublish(
                    "ex.direct",
                    "info",
                    null,
                    "Message with routing key info.".getBytes());

                channel.basicPublish(
                    "ex.direct",
                    "warning",
                    null,
                    "Message with routing key warning.".getBytes());

                channel.basicPublish(
                    "ex.direct",
                    "error",
                    null,
                    "Message with routing key error.".getBytes());

                //Wait until all published messages are confirmed
                channel.waitForConfirms();
	}

}
