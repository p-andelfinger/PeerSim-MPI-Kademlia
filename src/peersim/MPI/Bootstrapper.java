package peersim.MPI;

public class Bootstrapper {

	private static int numRanksDoneBootstrapping = 0;
	
	public static void sendDoneBootstrapping() {
		int message[] = new int[1];
		
		message[0] = CommunicatorConfig.SIGNAL_BOOTSTRAPPING_COMPLETE;
		for (int i = 0; i < Communicator.getSize(); i++)
			if (i != Communicator.getMyRank()) {
				Communicator.send(i, message);
				if (Logger.logEnabled)
					Logger.log("sending bootstrapping done signal to rank " + i);
			}
	}

	public static boolean isComplete() {
		return numRanksDoneBootstrapping == Communicator.getSize() - 1;
	}
	
	public static void incNumRanksDone() {
		numRanksDoneBootstrapping++;
	}
	
	public static int getNumRanksDone() {
		return numRanksDoneBootstrapping;
	}
}
