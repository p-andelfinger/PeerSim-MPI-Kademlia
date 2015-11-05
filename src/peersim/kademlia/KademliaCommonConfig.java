package peersim.kademlia;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.config.Configuration;

public class KademliaCommonConfig {

	public static final String VERSION = "0.2 rev. 1230";
	
	public static final String PAR_K = "K";
	public static final String PAR_ALPHA = "ALPHA";
	public static final String PAR_SIMPLE_BOOTSTRAP = "SIMPLE_BOOTSTR";
	public static final String PAR_SIMPLE_RT_CHECK = "SIMPLE_RT_CHECK";
	public static final String PAR_TIMEOUT = "TO";
	public static final String PAR_PLOT = "PLOT_LOOKUPS";
	public static final String PAR_MEASURE_SIMPLE_LOOKUPS = "SIMPLE_MEAS";
	public static final String PAR_RT_CHECK_INTERVAL = "RT_CHECK_INTVL";
	public static final String PAR_BUCKET_AGING_TIME = "BUCKET_AGING";
	public static final String PAR_PEER_AGING_TIME = "PEER_AGING";
	
	public static final String PREFIX_CONTROL_TRAFFIC = "control.2traffic";
	
	public static int K = 8; 
	public static int ALPHA = 1; 
	public static boolean SIMPLE_BOOTSTRAP = false;
	public static boolean SIMPLE_RT_CHECK = false;
	public static int TIMEOUT_DURATION = 1000;
	public static boolean PLOT_LOOKUPS = false;
	public static boolean MEASURE_SIMPLE_LOOKUPS = false;
	/**
	 * after this interval (in [ms]) the routing table checks for each bucket
	 * whether or not it needs to be refreshed.
	 * 
	 * @see RoutingTable#checkBuckets()
	 */
	public static long ROUTING_TABLE_CHECK_INTERVAL = 5 * 60 * 1000;
	
	public static final int PING_TIMEOUT_DELAY = 1000;
	
	/**
	 * if the contents of any {@link KBucket} do not change for this time in
	 * [ms], the bucket needs to be refreshed.
	 * 
	 * @see RoutingTable#checkBuckets()
	 */
	public static long BUCKET_AGING_TIME = 15 * 60 * 1000;

	/**
	 * time in [s] until a peer becomes questionable, if not answering to
	 * requests (= 15 min)
	 */
	public static int PEER_AGING_TIME = 15 * 60;
	
	public static void loadKademliaConfig(String prefix) {
		KademliaCommonConfig.SIMPLE_BOOTSTRAP = Configuration.getBoolean(prefix
				+ "." + PAR_SIMPLE_BOOTSTRAP,
				KademliaCommonConfig.SIMPLE_BOOTSTRAP);
		KademliaCommonConfig.SIMPLE_RT_CHECK = Configuration.getBoolean(
				prefix + "." + PAR_SIMPLE_RT_CHECK,
				KademliaCommonConfig.SIMPLE_RT_CHECK);
		KademliaCommonConfig.K = Configuration.getInt(prefix + "." + PAR_K,
				KademliaCommonConfig.K);
		KademliaCommonConfig.ALPHA = Configuration.getInt(prefix + "."
				+ PAR_ALPHA, KademliaCommonConfig.ALPHA);
		KademliaCommonConfig.TIMEOUT_DURATION = Configuration.getInt(prefix
				+ "." + PAR_TIMEOUT, KademliaCommonConfig.TIMEOUT_DURATION);
		KademliaCommonConfig.PLOT_LOOKUPS = Configuration.getBoolean(prefix
				+ "." + PAR_PLOT, KademliaCommonConfig.PLOT_LOOKUPS);
		KademliaCommonConfig.MEASURE_SIMPLE_LOOKUPS = Configuration.getBoolean(prefix
				+ "." + PAR_MEASURE_SIMPLE_LOOKUPS, KademliaCommonConfig.MEASURE_SIMPLE_LOOKUPS);
		KademliaCommonConfig.ROUTING_TABLE_CHECK_INTERVAL = Configuration.getLong(prefix
				+ "." + PAR_RT_CHECK_INTERVAL, 5) * 60 * 1000;
		KademliaCommonConfig.BUCKET_AGING_TIME = Configuration.getLong(prefix
				+ "." + PAR_BUCKET_AGING_TIME, 15) * 60 * 1000;
		

		KademliaCommonConfig.PEER_AGING_TIME = Configuration.getInt(prefix
				+ "." + PAR_PEER_AGING_TIME, 15) * 60;
		
		printConfig(prefix);
	}

	private static void printConfig(String prefix) {
		System.out.println("#Loading Kademlia Config for prefix \"" + prefix + "\":");
		printParam("Version", VERSION);
		printParam(PAR_SIMPLE_BOOTSTRAP, SIMPLE_BOOTSTRAP);
		printParam(PAR_SIMPLE_RT_CHECK, SIMPLE_RT_CHECK);
		printParam(PAR_K, K);
		printParam(PAR_ALPHA, ALPHA);
		printParam(PAR_TIMEOUT, TIMEOUT_DURATION);
		printParam(PAR_PLOT, PLOT_LOOKUPS);
		printParam(PAR_MEASURE_SIMPLE_LOOKUPS, MEASURE_SIMPLE_LOOKUPS);
		printParam(PAR_RT_CHECK_INTERVAL, ROUTING_TABLE_CHECK_INTERVAL / (60*1000));
		printParam(PAR_BUCKET_AGING_TIME, BUCKET_AGING_TIME / (60*1000));
		printParam(PAR_PEER_AGING_TIME, PEER_AGING_TIME / 60);
	}

	private static void printParam(String argName, Object arg) {
		System.out.println(String.format("#%20s = %s", argName, arg));
	}
}
