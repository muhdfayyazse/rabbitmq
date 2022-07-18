package rabbitmq.demos.headers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class HeadersDemo {

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
                "ex.headers",
                BuiltinExchangeType.HEADERS,
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
            
            Map<String, Object> argsQueue1 = new HashMap<String, Object>();
            argsQueue1.put("x-match","all");
            argsQueue1.put("job","convert");
            argsQueue1.put("format","jpeg");

            channel.queueBind(
                "my.queue1",
                "ex.headers",
                "",
                argsQueue1);

            Map<String, Object> argsQueue2 = new HashMap<String, Object>();
            argsQueue2.put("x-match","any");
            argsQueue2.put("job","convert");
            argsQueue2.put("format","jpeg");
            
            channel.queueBind(
                "my.queue2",
                "ex.headers",
                "",
                argsQueue2);
            
           
            AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
            Map<String, Object> argsMsg1 = new HashMap<String, Object>();
            argsMsg1.put("job","convert");
            argsMsg1.put("format","jpeg");
            
            propBuilder.headers(argsMsg1);
            
            AMQP.BasicProperties propsMsg1 = propBuilder.build();

            channel.basicPublish(
                "ex.headers",
                "",
                propsMsg1,
                "Message 1".getBytes());
            
            propBuilder = new AMQP.BasicProperties.Builder();
            Map<String, Object> argsMsg2 = new HashMap<String, Object>();
            argsMsg2.put("job","convert");
            argsMsg2.put("format","bmp");
            
            propBuilder.headers(argsMsg2);

            AMQP.BasicProperties propsMsg2 = propBuilder.build();

            channel.basicPublish(
                "ex.headers",
                "",
                propsMsg2,
                "Message 2".getBytes());

            //Wait until all published messages are confirmed
            channel.waitForConfirms();
	}
}
