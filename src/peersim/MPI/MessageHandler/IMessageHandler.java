package peersim.MPI.MessageHandler;

public interface IMessageHandler {
	public void handle(int[] message, int source);
	
	public void handle(float[] message, int source);
}
