package peersim.kademlia;

import java.util.ArrayList;
import java.util.Locale;

import jsc.distributions.Normal;
import jsc.distributions.Weibull;
import peersim.MPI.Bootstrapper;
import peersim.MPI.Communicator;
import peersim.MPI.CommunicatorConfig;
import peersim.MPI.Statistics;
import peersim.MPI.Logger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.events.PeerOfflineEvent;
import peersim.kademlia.events.PeerOnlineEvent;

public class StateBuilder implements peersim.core.Control {

	private static final String PAR_PROT = "protocol";
	private static final String PAR_PEER_COUNT = "PEER_COUNT";

	private String prefix;
	private int kademliaid;

	private int numoflocations;
	private ArrayList<Location> locs;

	private PeerCreatorNode creatorNode;

	private Weibull sessionLengthDistribution;
	/** timespan between the shutdown of one peer and the startup of the next one in [s]*/
	private double iaStep = 1;
	
	public StateBuilder(String prefix) {
		this.prefix = prefix;
		kademliaid = Configuration.getPid(this.prefix + "." + PAR_PROT);
		KademliaProtocol.kademliaid = kademliaid;

		loadSessionAndIaTimes();
		
		loadLocations();
	}

	private void loadLocations() {
		// load locations
		numoflocations = Configuration.getInt(this.prefix + "."
				+ "NUM_LOCATIONS", 0);

		locs = new ArrayList<Location>();
		for (int i = 0; i < numoflocations; i++) {
			int peer_count = Configuration.getInt(this.prefix + "." + i
					+ "PEER_COUNT", 0) / Communicator.getSize();
			double nat_ratio = Configuration.getDouble(this.prefix + "." + i
					+ "NAT_RATIO", 0);
			ArrayList<Normal> rtt_distributions = new ArrayList<Normal>();
			for (int j = 0; j < numoflocations; j++) {
				double rtt_mean = Configuration.getDouble(this.prefix + "." + i
						+ "RTT_MEAN_" + j, 200);
				double rtt_sd = Math.sqrt(Configuration.getDouble(this.prefix
						+ "." + i + "RTT_VAR_" + j, 50));
				Normal rtt_distribution = new Normal(rtt_mean, rtt_sd);
				rtt_distribution.setSeed(CommonState.r.getLastSeed());
				rtt_distributions.add(rtt_distribution);
			}
			Location loc = new Location(i, peer_count, nat_ratio);
			loc.setRttDistributions(rtt_distributions);

			locs.add(loc);
		}

		System.out.println("numoflocations= " + locs.size());
		for (Location loc : locs) {
			System.out.println("#Peers    = " + loc.getPeerCount());
			System.out.println("NAT ratio = " + loc.getNatRatio());
			System.out.println("RTT mean  = "
					+ loc.rtt_distributions.get(0).mean() + "ms");
			System.out.println("RTT variance  = "
					+ loc.rtt_distributions.get(0).variance());
		}
	}

	private void loadSessionAndIaTimes() {
		// load interarrival and session length distributions
		double session_scale = Configuration.getDouble(this.prefix + "."
				+ "SESSION_SCALE", 5);
		double session_shape = Configuration.getDouble(this.prefix + "."
				+ "SESSION_SHAPE", 0.6);
		sessionLengthDistribution = new Weibull(session_scale, session_shape);
		sessionLengthDistribution.setSeed(CommonState.r.getLastSeed());

		long peerNumber = Configuration.getLong(this.prefix + "."
				+ PAR_PEER_COUNT, 5) / Communicator.getSize();
		
		this.iaStep = (sessionLengthDistribution.mean() / ((double) peerNumber));
		
		Logger.log("iaStep is " + this.iaStep);
		
		System.out.println(String.format("Session Length [s]: %s ", sessionLengthDistribution.getClass().getSimpleName()));
		System.out.println(String.format(" mean: %.1f, variance: %.1f", sessionLengthDistribution.mean(), sessionLengthDistribution.variance()));

		System.out.println(String.format("Interarrival Time [s]: %s ", "constant rate"));
		System.out.println(String.format(" mean: %.1f, variance: %.1f", iaStep, 0.0));
	}

	/**
	 * InterArrivalTime must be converted to ms because shape and scale is
	 * designed for sec
	 * 
	 * @return
	 */
	private long calcInterArrivalTime() {
		return (long) (iaStep * 1000.0);
	}
	
	/**
	 * SeesionTime must be converted to ms because shape and scale is
	 * designed for sec
	 * 
	 * @return
	 */
	private long calcSessionTime() {
		double d = sessionLengthDistribution.random() * 1000;
		return (long) d;
	}

	public boolean execute() {
		Locale.setDefault(Locale.US);
		
		// init all peers created before simulation start
		initExistingPeers();
		
		// prepare Churn: peers join and leave
		generateChurn();
		
		Statistics.init();

		return false;
	}

	private void initExistingPeers() {
		// init existing peers: generate id, sessionLength and offline event
		if(Logger.logEnabled)
			Logger.log("Network.size() is " + Network.size());
		
		// add peers that stay alive for an amount of time as if they had existed before the simulation started
 		long endTime = -14 * 24 * 3600 * 1000;
		long curTime = 0;
		
		int numPeers = 0;
		while(numPeers < Network.size()) {
			long sessionLength = calcSessionTime();

			if(curTime + sessionLength >= 0 || curTime < endTime) {
				Node node = (Node) Network.get(numPeers);
				RemotePeer peer = (RemotePeer) (node.getProtocol(KademliaProtocol.kademliaid));
				int id = Util.chooseRandomPeerId();
				
				long sessionEnd = curTime + sessionLength;
				if(curTime < endTime) {
					sessionEnd = sessionLength; 
				}
				
				
				peer.init(node, id, sessionEnd);
				
				
				Network.addIPeer(peer);
				
				PeerOfflineEvent poff = new PeerOfflineEvent();
				EDSimulator.add(sessionEnd, poff, peer.toNode(), KademliaProtocol.kademliaid);
				numPeers++;
			}
			
			curTime -= calcInterArrivalTime();
			
			
		}
		/* for (int i = 0; i < Network.size(); ++i) {
			
			Node node = (Node) Network.get(i);
			RemotePeer peer = (RemotePeer) (node.getProtocol(KademliaProtocol.kademliaid));
			int id = Util.chooseRandomPeerId();
			long sessionLength = calcSessionTime();
			
			
			peer.init(node, id, sessionLength);
			//Logger.log("adding node " + node + ", id " + id);
			Network.addIPeer(peer);
			
			PeerOfflineEvent poff = new PeerOfflineEvent();
			EDSimulator.add(sessionLength, poff, peer.toNode(), KademliaProtocol.kademliaid);
		} */

		// assign location properties to nodes
		int nodeid = 0;
		for (int j = 0; j < locs.size(); j++) {
			Location loc = locs.get(j);
			
			for (int i = 0; i < loc.getPeerCount(); i++) {
				RemotePeer peer = Util.getPeer(nodeid + i);
				
				peer.setLocation(loc);
			}
			nodeid += loc.getPeerCount();
		}
		
		
		if(Logger.logEnabled)
			Logger.log("bootstrapping...");
		Statistics.fineClockStart(Statistics.FINE_CLOCK_BOOTSTRAPPING);
		// bootstrap peers
		for (int i = 0; i < Network.size(); i++) {
			RemotePeer peer = Util.getPeer(i);
			if (peer == null) 
				continue;
			peer.bootstrap();
			//Logger.log("bootstrapped peer " + i + " of " + Network.size() + ", id: " + peer.getId());
		}
		Bootstrapper.sendDoneBootstrapping();
		
		if(Communicator.getSize() > 1) {
			do {
				Communicator.receive(null);
			} while(!Bootstrapper.isComplete());
		}
		
		Statistics.fineClockFinish(Statistics.FINE_CLOCK_BOOTSTRAPPING);
		Statistics.init();
		
		if(Logger.logEnabled)
			Logger.log("done bootstrapping");
		
		Statistics.fineClockStart(Statistics.FINE_CLOCK_SIMULATION);

		EDSimulator.printNumEvents();
	}

	private void generateChurn() {
		
		// add a PeerCreatorNode to the network.
		// this node is needed for dynamically adding new peers later on,
		// as it will receive the peer creation events
		this.creatorNode = new PeerCreatorNode();
		Network.add(creatorNode);
		
		long endTime = CommonState.getEndTime();
		long curTime = 0;
		
		while (curTime < endTime) {
			// generate peer creation events
			long sessionLength = calcSessionTime();
			Location loc = chooseLocation();
			PeerOnlineEvent pone = new PeerOnlineEvent(loc, sessionLength);
			EDSimulator.add(curTime, pone, creatorNode,
					PeerCreatorNode.EVENT_HANDLER_ID);
	
			// calculate next interarrival time
			curTime += calcInterArrivalTime();
		}
		

	}

	private Location chooseLocation() {
		int peersTotal = 0;
		for (Location loc : locs) {
			peersTotal += loc.getPeerCount();
		}

		int number = CommonState.r.nextInt(peersTotal);

		int peers = 0;
		for (Location loc : locs) {
			peers += loc.getPeerCount();
			if (number < peers)
				return loc;
		}
		return locs.get(locs.size() - 1);
	}

	public class Location {
		private int id;
		private int peerCount;
		private double natRatio;

		private ArrayList<Normal> rtt_distributions;

		public Location(int id, int peerCount, double natRatio) {
			this.id = id;
			this.peerCount = peerCount;
			this.natRatio = natRatio;
		}

		public int getPeerCount() {
			return this.peerCount;
		}

		public double getNatRatio() {
			return this.natRatio;
		}

		public void setRttDistributions(ArrayList<Normal> distributions) {
			this.rtt_distributions = distributions;
		}
		
		public long getNextLatency(IPeer to) {
			long rtt;
			int maxRtt = 200;
			
			if(to instanceof RemoteRankPeer) {
				rtt = (CommonState.r.nextInt(maxRtt - CommunicatorConfig.MIN_REMOTE_RTT) + CommunicatorConfig.MIN_REMOTE_RTT);
			} else {
				rtt = (CommonState.r.nextInt(maxRtt - CommunicatorConfig.MIN_LOCAL_RTT) + CommunicatorConfig.MIN_LOCAL_RTT);
			}
			
			return rtt / 2;
		}
	}
}
