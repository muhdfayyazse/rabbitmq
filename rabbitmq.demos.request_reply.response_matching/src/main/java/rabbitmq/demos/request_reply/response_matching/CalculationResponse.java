package rabbitmq.demos.request_reply.response_matching;

public class CalculationResponse {
	public Integer Result;

	@Override
    public String toString()
    {
        return Result.toString();
    }
}
