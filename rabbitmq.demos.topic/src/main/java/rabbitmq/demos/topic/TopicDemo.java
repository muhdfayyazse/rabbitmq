package rabbitmq.demos.topic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TopicDemo {

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
                "ex.topic",
                BuiltinExchangeType.TOPIC,
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

            channel.queueDeclare(
                "my.queue3",
                true,
                false,
                false,
                null);

            channel.queueBind("my.queue1", "ex.topic", "*.image.*");
            channel.queueBind("my.queue2", "ex.topic", "#.image");
            channel.queueBind("my.queue3", "ex.topic", "image.#");

            channel.basicPublish(
                "ex.topic",
                "convert.image.bmp",
                null,
                "Routing key is convert.image.bmp".getBytes());

            channel.basicPublish(
                "ex.topic",
                "convert.bitmap.image",
                null,
                "Routing key is convert.bitmap.image".getBytes());

            channel.basicPublish(
                "ex.topic",
                "image.bitmap.32bit",
                null,
                "Routing key is image.bitmap.32bit".getBytes());

            //Wait until all published messages are confirmed
            channel.waitForConfirms();
	}

}
