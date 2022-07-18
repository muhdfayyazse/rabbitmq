package rabbitmq.demos.request_reply.response_matching;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Requestor {

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		
		final ConcurrentHashMap<String, CalculationRequest> waitingRequests = new ConcurrentHashMap<String, CalculationRequest>();
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setVirtualHost("/");
		factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");		
        
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        
        Consumer consumer = new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {	        	 
	        	 String requestId = new String((byte[])properties.getHeaders().get(Constants.RequestIdHeaderKey));

                 if (waitingRequests.containsKey(requestId))
                 {
                	 CalculationRequest request = waitingRequests.get(requestId);
                     String messageData = new String(body);
                     
                     ObjectMapper mapper = new ObjectMapper();
                     CalculationResponse response = mapper.readValue(messageData, CalculationResponse.class);                      

                     System.out.println("Calculation result: " + request.toString() +"="+ response.toString());
                 }	             
	         }
	     };       

       channel.basicConsume("responses", true, consumer);
       
       System.out.println("Press enter to send requests");
       Scanner reader = new Scanner(System.in);
       reader.nextLine();

       sendRequest(waitingRequests, channel, new CalculationRequest(2, 4, OperationType.Add));
       sendRequest(waitingRequests, channel, new CalculationRequest(8, 6, OperationType.Subtract));
       sendRequest(waitingRequests, channel, new CalculationRequest(20, 7, OperationType.Add));            
       sendRequest(waitingRequests, channel, new CalculationRequest(50, 8, OperationType.Subtract));

       System.out.println("Press enter to exit");
       reader.nextLine();

       channel.close();
       conn.close();

	}
	
	private static void sendRequest(
			ConcurrentHashMap<String, CalculationRequest> waitingRequest, 
            Channel channel, CalculationRequest request) throws IOException
        {
			UUID uuid = UUID.randomUUID();
            String requestId = uuid.toString();
            
            ObjectMapper mapper = new ObjectMapper();
            String requestData = mapper.writeValueAsString(request);

            waitingRequest.put(requestId, request);
            
            AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
            Map<String, Object> argsMsg = new HashMap<String, Object>();
            argsMsg.put(Constants.RequestIdHeaderKey,requestId.getBytes());            
            
            propBuilder.headers(argsMsg);
            
            AMQP.BasicProperties propsMsg = propBuilder.build();

            channel.basicPublish(
                "", 
                "requests",
                propsMsg, 
                requestData.getBytes());
        }

}
