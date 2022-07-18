package rabbitmq.demos.request_reply.response_queue_name;

public class CalculationResponse {
	public Integer Result;

	@Override
    public String toString()
    {
        return Result.toString();
    }
}
