package peersim.MPI;

public class Logger {
	public static final boolean logEnabled = true; // no getter? burn the witch!
	
	public static void log(String msg) {
		System.err.println("+++ " + Communicator.getMyRank() + ": " + msg);
	}
}
