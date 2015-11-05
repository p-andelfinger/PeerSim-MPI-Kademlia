package peersim.kademlia;

import peersim.MPI.Communicator;
import peersim.MPI.Statistics;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.util.IncrementalStats;

/**
 * This class implements a simple observer of search time and hop average in finding a node in the network
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaObserver implements Control {
	
	/**
	 * keep track of findNode queries with timeout
	 */
	public static long timeoutfindNode = 0;
	
	/**
	 * keep track of findNode queries
	 */
	public static long findNodequeries = 0;
	/**
	 * number of peers dynamically created during simulation
	 */
	public static int peersAdded = 0;
	/**
	 * number of peers dynamically removed during simulation
	 */
	public static int peersRemoved = 0;

	/**
	 * keep statistics of the number of lookup queries sent
	 */
	public static IncrementalStats lookupCost = new IncrementalStats();
	
	/**
	 * amount of pings to keep statistics of the total number of maintenance
	 * queries (= ping + lookup queries). These queries are sent to detect
	 * stale entries in the routing table and find replacements for these
	 * entries.
	 */
	public static IncrementalStats pingCost = new IncrementalStats();
	
	/**
	 * like lookupCost, but is not reseted every observation period
	 */
	public static IncrementalStats sumLookupCost = new IncrementalStats();
	
	/**
	 * like pingCost, but is not reseted every observation period
	 */
	public static IncrementalStats sumPingCost = new IncrementalStats();
	
	/**
	 * like timeStore, but is not reseted every observation period
	 */
	public static IncrementalStats sumtimeStore = new IncrementalStats();

	/**
	 * keep statistics of the time every message delivered.
	 */
	public static IncrementalStats timeStore = new IncrementalStats();

	/**
	 * keep statistic of number of message delivered
	 */
	public static IncrementalStats msg_deliv = new IncrementalStats();

	/**
	 * keep statistic of number of find operation
	 */
	public static IncrementalStats lookups = new IncrementalStats();

//	/** Parameter of the protocol we want to observe */
//	private static final String PAR_PROT = "protocol";

//	/** Protocol id */
//	private int pid;
//
//	/** Prefix to be printed in output */
//	private String prefix;
	
	/** Previous msg count for preventing NaN + infinity output */
	private int prevmsgcount = 0;

	public KademliaObserver(String prefix) {
//		this.prefix = prefix;
//		pid = Configuration.getPid(prefix + "." + PAR_PROT);
	}
	
	public static void forwardStats() {
		int totalPeers = Network.size();
		int onlinePeers = totalPeers;
    	int pcost = (int) sumPingCost.getAverage();
  		/* Statistics.transmitStats(onlinePeers, totalPeers, (int) msg_deliv.getSum(),
   				(int) sumLookupCost.getMin(), (int) sumLookupCost.getAverage(), (int) sumLookupCost.getMax(), pcost,
   				(int) sumtimeStore.getMin(), (int) sumtimeStore.getAverage(), (int) sumtimeStore.getMax(), timeoutfindNode, findNodequeries, peersAdded, peersRemoved); */
  		
  		Statistics.transmitStats(onlinePeers, totalPeers, (float)msg_deliv.getSum(),
   				(float)sumLookupCost.getMin(), (float)sumLookupCost.getAverage(), (float)sumLookupCost.getMax(), pcost,
   				(float)sumtimeStore.getMin(), (float)sumtimeStore.getAverage(), (float)sumtimeStore.getMax(), timeoutfindNode, findNodequeries, peersAdded, peersRemoved);

	}

    private boolean firstrun = true;

	/**
	 * print the statistical snapshot of the current situation
	 * 
	 * @return boolean always false
	 */
	public boolean execute() {
		// get the network size
		if(Communicator.getMyRank() != 0)
			return false;
		
		int totalPeers = Network.size();
		int onlinePeers = totalPeers;
		
//		for (int i = 0; i < Network.size(); i++) {
//			RemotePeer peer = Util.getPeer(i);
//			if (peer == null) {
//				totalPeers--;
//				continue;
//			}
//			if (peer.isOnline())
//				onlinePeers++;
//		}

        if (firstrun) {
        	System.out.println(String.format("#\tpeers\t\tlookups\tmsgs/lookup\t\t\tlookup time [ms]"));
        	System.out.println(String.format("#Time(s)online\ttotal\t\t"
                    + "min\tavg\tmax\t"
                    + "+pings\t"
                    + "min\tavg\tmax"));
        	System.out.println(String.format("%d\t%d\t%d\t%d\t" + "%d\t%d\t%d\t" + "%d\t" + "%d\t%d\t%d",
                    0, onlinePeers, totalPeers, 0, 0, 0, 0, 0, 0, 0, 0));
            firstrun = false;
		} else if ((int) msg_deliv.getSum() > prevmsgcount) {
			int pcost = (int) pingCost.getAverage();

			String s = String.format("%d\t%d\t%d\t%d\t"
					+ "%3.1f\t%3.1f\t%3.1f\t" + "%3.1f\t" + "%d\t%d\t%d",
					CommonState.getTime() / 1000, onlinePeers, totalPeers,
					(int) msg_deliv.getSum(), lookupCost.getMin(),
					lookupCost.getAverage(), lookupCost.getMax(),
					lookupCost.getAverage() + pcost, (int) timeStore.getMin(),
					(int) timeStore.getAverage(), (int) timeStore.getMax());

			prevmsgcount = (int) msg_deliv.getSum();
			String lstatstics = String.format(" %d | %d : %d | %d | %d : %d",
					Lookup.OPERATION_ID_GENERATOR,
					Lookup.LOOKUPS_FINISHED, Lookup.LOOKUPS_CANCELED, Lookup.DESTRUCTOR_COUNTER, Lookup.LOOKUPS_LOST_PACKETS, Lookup.NOT_EVEN_STARTED);
			System.out.println(s + lstatstics);
        }  /* else {
        	String s = String.format("%d\t%d\t%d\t%d\t" + "%d\t%d\t%d\t" + "%d\t" + "%d\t%d\t%d",
                CommonState.getTime()/1000, onlinePeers, totalPeers, 0, 0, 0, 0, 0, 0, 0, 0);
			prevmsgcount = (int) msg_deliv.getSum();
			String lstatstics = String.format(" %d | %d : %d | %d | %d : %d",
					Lookup.OPERATION_ID_GENERATOR,
					Lookup.LOOKUPS_FINISHED, Lookup.LOOKUPS_CANCELED, Lookup.DESTRUCTOR_COUNTER, Lookup.LOOKUPS_LOST_PACKETS, Lookup.NOT_EVEN_STARTED);
    	}
        
        lookupCost.reset();
        pingCost.reset();
        timeStore.reset();
    	int pcost = (int) sumPingCost.getAverage();
        
        long time = CommonState.getTime();
        if (time != 0 && CommonState.getTime() >= (CommonState.getEndTime()-1)) {
       		Statistics.transmitStats(onlinePeers, totalPeers, (int) msg_deliv.getSum(),
       				(int) sumLookupCost.getMin(), (int) sumLookupCost.getAverage(), (int) sumLookupCost.getMax(), pcost,
       				(int) sumtimeStore.getMin(), (int) sumtimeStore.getAverage(), (int) sumtimeStore.getMax(), timeoutfindNode, findNodequeries, peersAdded, peersRemoved);
        	
        } */
        
        
		return false;
	}
}
