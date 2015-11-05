package peersim.MPI;

import peersim.config.Configuration;

public class CommunicatorConfig {
	
	public static int MAX_MESSAGE_LENGTH;
	public static int MAX_RECEIVED_PEERS;
	public static int MIN_RECEIVED_PEERS;
	public static int RANDOM_ONLINE_PEERS_PER_MESSAGE;
	
	public static final int CONTENT_TYPE_INTS = 0;
	public static final int CONTENT_TYPE_FLOATS = 1;

	// message types
	public static final int REQUEST_RANDOM_ONLINE_PEER_MESSAGE_ID = 0;
	public static final int RESPONSE_RANDOM_ONLINE_PEER_MESSAGE_ID = 1;
	public static final int SIGNAL_BOOTSTRAPPING_COMPLETE = 2;
	public static final int FIND_NODE_REQUEST_EVENT = 3;
	public static final int FIND_NODE_RESPONSE_EVENT = 4;
	public static final int NULL_MESSAGE = 5;
	public static final int QUEUE_EMPTY = 6;
	public static final int REVOKE_QUEUE_EMPTY = 7;
	public static final int FIND_NODE_TIMEOUT_EVENT = 8;
	public static final int STATISTICS = 9;
	public static final int PING_REQUEST_EVENT = 10;
	public static final int PING_RESPONSE_EVENT = 11;
	public static final int PING_TIMEOUT_EVENT = 12;
	
	// optimization parameters
	public static boolean ID_BASED_PARTITIONING;
	public static boolean FAST_LOCAL_PINGS; // changes results: if true, results are closer to KJ-implementation, but if false, behavior is closer to what the transmission client does
	public static boolean FAST_LOCAL_FIND_NODE_TIMEOUT;
	public static long GRACE_PERIOD_MS;
	
	public static boolean LAZY_NULL_MESSAGES = false;
	
	public static final int MIN_LOCAL_RTT = 20;
	public static final int MIN_REMOTE_RTT = 20;
	
	// --------------------------------------------------------
	
	private static final String PREFIX = "mpi.comm.";

	public static void setupCommunicator() {
		MAX_MESSAGE_LENGTH = Configuration.getInt(PREFIX + "maxMessageLength", 10240);
		MAX_RECEIVED_PEERS = Configuration.getInt(PREFIX + "maxRecvdPeers", 100);
		MIN_RECEIVED_PEERS = Configuration.getInt(PREFIX + "minRecvdPeers", 90);
		RANDOM_ONLINE_PEERS_PER_MESSAGE = Configuration.getInt(PREFIX + "randomOnlinePeersPerMessage", 100);
		
		ID_BASED_PARTITIONING = Configuration.getBoolean(PREFIX + "idBasedPartitioning", true);
		FAST_LOCAL_PINGS = Configuration.getBoolean(PREFIX + "fastLocalPings", false);
		FAST_LOCAL_FIND_NODE_TIMEOUT = Configuration.getBoolean(PREFIX + "fastLocalFindNodeTimeout", true);
		GRACE_PERIOD_MS = Configuration.getLong(PREFIX + "gracePeriodMs", 1000);
		
	}
}
