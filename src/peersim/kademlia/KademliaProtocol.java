package peersim.kademlia;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import peersim.MPI.Communicator;
import peersim.MPI.CommunicatorConfig;
import peersim.MPI.Logger;
import peersim.MPI.Statistics;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.kademlia.StateBuilder.Location;
import peersim.kademlia.events.CheckRoutingTableEvent;
import peersim.kademlia.events.FindNodeEvent;
import peersim.kademlia.events.PeerOfflineEvent;
import peersim.kademlia.events.PingEvent;

public class KademliaProtocol implements IPeer {



	/**
	 * The ID of the Kademlia Protocol. Used to access the {@link RemotePeer}
	 * from a {@link Node}.
	 */
	public static int kademliaid;

	/**
	 * This peer converted as a {@link RemotePeer} which can be transmitted to
	 * other peers.
	 */
	private RemotePeer remPeer = null;

	/** the id of this peer */
	private int nId;

	/** routing table of this peer */
	public RoutingTable routingTable;

	private Location location;

	private boolean isBehindNat = false;

	private ArrayList<Lookup> runningLookups = new ArrayList<Lookup>(5);

	private HashMap<Integer, Lookup> lookupIdToLookup = new HashMap<Integer, Lookup>();

	private long sessionEnd;



	public KademliaProtocol(RemotePeer rem) {
		setRemotePeer(rem);
	}

	public Node toNode() {
		return null;
	}

	public void processEvent(Node myNode, int pId, Object event) {
		
		// Logger.log("event at " + CommonState.getTime());
		if (event instanceof FindNodeEvent) {
			
			FindNodeEvent evt = (FindNodeEvent) event;

			
			switch (evt.getType()) {
			case FindNodeEvent.EVT_SRT_FINDNODE:
				onStartLookup(evt, pId);
				break;

			case FindNodeEvent.EVT_RCV_FINDNODE_RSP:
				onIncomingFindNodeResponse(pId, evt);
				break;

			case FindNodeEvent.EVT_RCV_FINDNODE_RQU:
				onIncomingFindNodeRequest(pId, evt);
				break;

			case FindNodeEvent.EVT_RCV_FINDNODE_TMO:
				onIncomingTimeout(pId, evt);
				break;

			default:
				System.out.println("Error! Unknown Message Type!");
			}
		} else if (event instanceof PeerOfflineEvent) {
			
			KademliaObserver.peersRemoved++;
			this.shutdown();
		} else if (event instanceof CheckRoutingTableEvent) {
			onCheckRoutingTable((CheckRoutingTableEvent) event);
		} else if (event instanceof PingEvent) {
			PingEvent evt = (PingEvent) event;
			switch (evt.getType()) {

			case PingEvent.EVT_PING:
				onIncomingPing(evt.source, evt.peerToAddId, evt.lastReceived);
				break;

			case PingEvent.EVT_PING_TMO:
				onIncomingPingTimeout(evt);
				break;

			case PingEvent.EVT_PING_RSP:
				onIncomingPingResponse(evt);
				break;
			}
		} else {
			Logger.log("BUG: unknown event type");
			System.exit(1);
		}

	}

	private void onCheckRoutingTable(CheckRoutingTableEvent event) {

		
		// schedule next routing table check
		generateNextRoutingTableCheck(false);

		if (routingTable.countPeers() == 0) {
			return;
		}
		routingTable.checkBuckets();
	}

	protected void generateNextRoutingTableCheck(boolean randomDelay) {
		if (this.sessionEnd >= CommonState.getTime()
				+ KademliaCommonConfig.ROUTING_TABLE_CHECK_INTERVAL) {
			CheckRoutingTableEvent evt = new CheckRoutingTableEvent();
			
			int delay = randomDelay ? CommonState.r.nextInt((int)KademliaCommonConfig.ROUTING_TABLE_CHECK_INTERVAL) : 0;

			Communicator.scheduleEvent(randomDelay ? delay : KademliaCommonConfig.ROUTING_TABLE_CHECK_INTERVAL,
					this.remPeer, evt);
		}
	}


	private void shutdown() {
		
		// cancel all lookups
		ArrayList<Lookup> temp = runningLookups;
		runningLookups = new ArrayList<Lookup>(0);

		for (Lookup lookup : temp) {
			lookup.cancel();
		}

		// set remote peer to null, shut it down and remove it from the network
		remPeer.shutdown();
		remPeer = null;
	}

	public void startLookup(int target, int pId) {
		
		KademliaObserver.lookups.add(1);

		// create find operation and add to operations array
		Lookup fop = new Lookup(this, target, CommonState.getTime());
		runningLookups.add(fop);
		lookupIdToLookup.put(fop.getLookupId(), fop);

		// start query
		fop.onStart(pId);
	}

	private void onStartLookup(FindNodeEvent m, int pId) {
		startLookup(m.target, pId);
	}

	private void onIncomingPingTimeout(PingEvent evt) {
		routingTable.onIncomingPingTimeout(evt.source, evt.peerToAddId,
				evt.lastReceived);
	}

	private void onIncomingPingResponse(PingEvent evt) {
		routingTable.onIncomingPingResponse(evt.source, evt.peerToAddId,
				evt.lastReceived);
	}

	private void onIncomingTimeout(int pId, FindNodeEvent evt) {
		routingTable.removePeer(evt.src);
		
		// do not process event if corresponding operation has already finished
		if (evt.operation.hasFinished()) {
			return;
		}

		evt.operation.onTimeout(evt, pId);
	}

	private void onIncomingFindNodeResponse(int pId, FindNodeEvent evt) {

		// add source node to routing table
		routingTable.notifyAboutIncResponse(evt.src);

		// do not process event if corresponding operation has already finished
		if (evt.operation.hasFinished()) {
			return;
		}

		// handle response in find operation
		evt.operation.onResponse(evt, pId);
	}

	private void onIncomingFindNodeRequest(int pId, FindNodeEvent evt) {

		if (!canReceiveFrom(evt.src)) {
			
			FindNodeEvent timeoutEvent = FindNodeEvent.createFindNodeTimeoutEvent(evt.operation, remPeer);
			Communicator.scheduleEvent(KademliaCommonConfig.TIMEOUT_DURATION, evt.src, timeoutEvent);
			
			return;
		}

		// add source node to routing table
		routingTable.notifyAboutIncRequest(evt.src);


		// get nodes closest to destNode
		List<IPeer> peers = this.routingTable.getClosestPeers(evt.target);

		sendFindNodeResponse(evt.src, pId, evt.operation, peers, evt.hops,
				evt.timestamp);
	}

	private Set<IPeer> bPeers = new HashSet<IPeer>();
	
	@Override
	public void bootstrap() {
		// number of buckets that should be created
		int bucketCount = 6;

		ArrayList<KBucket> buckets;

		do {
			// choose random peer that is online
			IPeer peer = Util.chooseRandomOnlinePeerGlobally();
			
			bPeers.add(peer);
			// put it into routing table
			routingTable.notifyAboutIncResponse(peer);

			// update buckets
			buckets = routingTable.getBuckets();
		} while (buckets.size() < bucketCount);


		// from BEP 5:
		/*
		 * Upon inserting the first node into its routing table and when
		 * starting up thereafter, the node should attempt to find the closest
		 * nodes in the DHT to itself. It does this by issuing find_node
		 * messages to closer and closer nodes until it cannot find any closer.
		 */
		
		startLookup(nId, KademliaProtocol.kademliaid);
	}

	/**
	 * Sets the peer's current Id.
	 * 
	 * @param tmp
	 *            BigInteger
	 */
	private void setRemotePeer(RemotePeer rem) {
		this.remPeer = rem;
		this.nId = rem.getId();

		routingTable = new RoutingTable(nId, this);
	}

	protected final void sendFindNodeRequest(IPeer dest, int pId, Lookup op,
			int hops) {
		
		Location srcLoc = remPeer.getLocation();
		
		long latency = srcLoc.getNextLatency(dest);
		
		if(CommunicatorConfig.FAST_LOCAL_FIND_NODE_TIMEOUT && !dest.canReceiveFrom(remPeer, CommonState.getTime() + latency)) {
			FindNodeEvent timeoutEvent = FindNodeEvent.createFindNodeTimeoutEvent(op, dest);
			Communicator.scheduleEvent(KademliaCommonConfig.TIMEOUT_DURATION, remPeer, timeoutEvent);
		} else {

			FindNodeEvent evt = FindNodeEvent.createFindNodeRequest(op, remPeer);
			Communicator.scheduleEvent(latency, dest, evt);
		}
	}

	protected final void sendFindNodeResponse(IPeer dest, int pId, ILookup op,
			List<IPeer> nodes, int hops, long requestSentAt) {

		Location srcLoc = remPeer.getLocation();

		if(dest.isOnline()) {
			FindNodeEvent evt = FindNodeEvent.createFindNodeResponse(op, remPeer,
					nodes, hops, requestSentAt);
			long latency = srcLoc.getNextLatency(dest);
			Communicator.scheduleEvent(latency, dest, evt);
		} else {
		}
		
	}

	public int getId() {
		return this.nId;
	}

	// @Override
	public void onIncomingPing(IPeer from, int peerToAddId, int lastReceived) {
	
		if (this.canReceiveFrom(from)) {
			
			routingTable.notifyAboutIncRequest(from);

			Location srcLoc = remPeer.getLocation();

			PingEvent evt = new PingEvent(PingEvent.EVT_PING_RSP, remPeer,
					peerToAddId, lastReceived);
			long latency = srcLoc.getNextLatency(from);
			Communicator.scheduleEvent(latency, from, evt);
			
			
		} else {

			PingEvent evt = new PingEvent(PingEvent.EVT_PING_TMO, remPeer,
					peerToAddId, lastReceived);
			Communicator.scheduleEvent(KademliaCommonConfig.PING_TIMEOUT_DELAY, from, evt);
		}
	}

	/**
	 * Pings a specific peer.
	 * 
	 * @param peer
	 *            the peer to ping.
	 * @return <code>true</code>, if the ping was successful, else false.
	 */
	public void startPing(IPeer dest, IPeer peerToAdd, int lastReceived) {
	
		Location srcLoc = remPeer.getLocation();
		long latency = srcLoc.getNextLatency(dest);
		
		PingEvent evt = new PingEvent(PingEvent.EVT_PING, remPeer, peerToAdd
				.getId(), lastReceived);
		Communicator.scheduleEvent(latency, dest, evt);
	}

	public boolean canReceiveFrom(IPeer sender) {
		return isOnline() && !this.isBehindNat;
	}

	@Override
	public boolean canReceiveFrom(IPeer sender, long atTime) {

		return isOnline() && !this.isBehindNat && this.sessionEnd > atTime;
	}

	public boolean isOnline() {
		return remPeer != null;
	}

	@Override
	public void setLocation(Location loc) {
		this.location = loc;
		// roll a dice to determine if peer is behind a NAT gateway or not
		this.isBehindNat = (CommonState.r.nextInt(100) < (loc.getNatRatio() * 100));
	}

	@Override
	public Location getLocation() {
		return this.location;
	}

	public void removeLookup(Lookup lookup) {
		this.runningLookups.remove(lookup);
	}


	public void setSessionEnd(long sessionEnd) {
		this.sessionEnd = sessionEnd;
	}

	public RemotePeer getRemotePeer() {
		return remPeer;
	}

	public Lookup getLookupById(int i) {
		return lookupIdToLookup.get(i);
	}
	
	public void notifyAboutPingRequest(IPeer src) {
		routingTable.notifyAboutIncRequest(src);
	}
	
	public boolean fastPing(KademliaProtocol dest) {
		long latency = location.getNextLatency(dest);
		
		if(!dest.canReceiveFrom(remPeer, CommonState.getTime() + latency)) {
			return false;
		} else {
			routingTable.notifyAboutIncResponse(dest);
			dest.notifyAboutPingRequest(remPeer);
			return true;
		}
	}


}
