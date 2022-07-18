package rabbitmq.demos.request_reply.response_queue_name;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Replier {

public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		
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
	             
	             String requestData = new String(body);
	             
	             ObjectMapper mapper = new ObjectMapper();
	             CalculationRequest request = mapper.readValue(requestData, CalculationRequest.class);
	             System.out.println("Request received:" + request.toString());

	             CalculationResponse response = new CalculationResponse();

	             if(request.Operation == OperationType.Add)
	             {
	                 response.Result = request.Number1 + request.Number2;
	             }
	             else if(request.Operation == OperationType.Subtract)
	             {
	                 response.Result = request.Number1 - request.Number2;
	             }                

	             String responseData = mapper.writeValueAsString(response);
	             
	             String requestId = new String((byte[])properties.getHeaders().get(Constants.RequestIdHeaderKey));
	             String responseQueueName = new String((byte[])properties.getHeaders().get(Constants.ResponseQueueHeaderKey));

	             AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
	             Map<String, Object> argsMsg = new HashMap<String, Object>();
	             argsMsg.put(Constants.RequestIdHeaderKey,requestId.getBytes());
	             
	             propBuilder.headers(argsMsg);
	             
	             AMQP.BasicProperties propsMsg = propBuilder.build();             
	           
	             channel.basicPublish(
	             "",
	             responseQueueName,
	             propsMsg,
	             responseData.getBytes());
	         }
	     };       

        channel.basicConsume("requests", true, consumer);

        System.out.println("Press enter to exit.");
        Scanner reader = new Scanner(System.in);
        reader.nextLine();

        channel.close();
        conn.close();
	}

}
