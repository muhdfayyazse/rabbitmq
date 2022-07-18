package rabbitmq.demos.request_reply.response_queue_name;

public class CalculationRequest {
	public Integer Number1;
    public Integer Number2;
    public OperationType Operation;

    public CalculationRequest()
    {
    }

    public CalculationRequest(Integer number1, Integer number2, OperationType operationType)
    {
        this.Number1 = number1;
        this.Number2 = number2;
        this.Operation = operationType;
    }

    @Override
    public String toString()
    {
        return Number1.toString() + (this.Operation == OperationType.Add ? "+":"-") + Number2.toString();
    }
}
