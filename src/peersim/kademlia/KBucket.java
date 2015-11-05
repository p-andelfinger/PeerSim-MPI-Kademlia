package peersim.kademlia;

import java.util.List;

import peersim.MPI.Communicator;
import peersim.MPI.CommunicatorConfig;
import peersim.MPI.Logger;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.RemotePeer.OfflinePeer;

public class KBucket implements Cloneable {

	// About Good, Questionable, and Bad peers:
	// taken from BEP 5 (http://www.bittorrent.org/beps/bep_0005.html):
	/*
	 * Not all nodes that we learn about are equal. Some are "good" and some are
	 * not. Many nodes using the DHT are able to send queries and receive
	 * responses, but are not able to respond to queries from other nodes. It is
	 * important that each node's routing table must contain only known good
	 * nodes. A good node is a node has responded to one of our queries within
	 * the last 15 minutes. A node is also good if it has ever responded to one
	 * of our queries and has sent us a query within the last 15 minutes. After
	 * 15 minutes of inactivity, a node becomes questionable. Nodes become bad
	 * when they fail to respond to multiple queries in a row. Nodes that we
	 * know are good are given priority over nodes with unknown status.
	 */
	
	private boolean pingPending = false;

	/** the peers within this bucket */
	private IPeer[] peers = null;

	/** the number of peers currently contained by this bucket */
	private byte entryCount;

	/**
	 * last time we received a message (request or response) from the i-th peer
	 * within {@link #peers}
	 * <ul>
	 * <li>value >= 0 means: answered to at least once to a response; value
	 * represents the timestamp of the last message received (request or
	 * response) in seconds [s]!
	 * <li>value = -1 means: never answered to a response (but was not yet
	 * pinged)
	 * <li>value = -2 means: never answered to a response and did not answer to
	 * a ping => BAD peer
	 * </ul>
	 * 
	 * @see #getIntTime()
	 * */
	private int[] lastReceived = null;

	/** last time this bucket was changed */
	private long lastChanged = -1;

	public KBucket() {
		peers = new IPeer[KademliaCommonConfig.K];
		entryCount = 0;
		lastReceived = new int[KademliaCommonConfig.K];
	}

	public void notifyAboutIncRequest(IPeer peer, RoutingTable routingTable) {
		// does peer already exist?
		for (int i = 0; i < entryCount; i++) {
			IPeer p = peers[i];
			if (p.getId() == peer.getId()) {
				// update only if we already received a response from peer
				if (lastReceived[i] >= 0) {
					
					updatePeer(i);
				} else {
				}
				return;
			}
		}

		// else add peer as questionable
		addPeer(peer, -1, routingTable);
	}

	public void notifyAboutIncResponse(IPeer peer, RoutingTable routingTable) {
		for (int i = 0; i < entryCount; i++) {
			IPeer p = peers[i];
			if (p.getId() == peer.getId()) {
				
				updatePeer(i);
				return;
			}
		}

		// else add peer with current timestamp as it was a response
		addPeer(peer, CommonState.getKademliaSecTime(), routingTable);
	}

	/**
	 * Checks, if peer <code>i</code> is considered being a <b>good</b> peer.
	 * <p>
	 * For being a <b>good</b> peer, exactly two conditions have to be met:
	 * <ol>
	 * <li>the peer must have answered to a response at least once (ever)
	 * <li>we must have received a message from him (request or response) in the
	 * last 15 mins
	 * </ol>
	 * 
	 * @param i
	 * @return
	 */
	private boolean isGood(int i) {
		int ts = lastReceived[i];
		if (ts < 0)
			return false;
		int now = CommonState.getKademliaSecTime();
		if (now < ts)
			return now - ts + Integer.MAX_VALUE < KademliaCommonConfig.PEER_AGING_TIME;
		else {
			return now - ts < KademliaCommonConfig.PEER_AGING_TIME;
		}
	}

	private boolean isQuestionable(int i) {
		return (lastReceived[i] == -1 || !isGood(i));
	}

	private boolean isBad(int i) {
		return lastReceived[i] == -2;
	}

	private void updatePeer(int i) {
		
		
		this.lastChanged = CommonState.getTime();
		lastReceived[i] = CommonState.getKademliaSecTime();
	}
	
	private void addPeer(int peerToAddId, int timeInSeconds,
			RoutingTable routingTable) {
		IPeer peerToAdd = Communicator.getRemoteRankPeer(peerToAddId);
		if (peerToAdd == null) {
			
			return;
		}
		addPeer(peerToAdd, timeInSeconds, routingTable);
	}

	/*
	 * BEP 5: When the bucket is full of good nodes, the new node is simply
	 * discarded.
	 * 
	 * If any nodes in the bucket are known to have become bad, then one is
	 * replaced by the new node.
	 * 
	 * If there are any questionable nodes in the bucket that have not been seen
	 * in the last 15 minutes, the least recently seen node is pinged. If the
	 * pinged node responds then the next least recently seen questionable node
	 * is pinged until one fails to respond or all of the nodes in the bucket
	 * are known to be good.
	 * 
	 * If a node in the bucket fails to respond to a ping, it is suggested to
	 * try once more before discarding the node and replacing it with a new good
	 * node.
	 */
	private boolean addPeer(IPeer peerToAdd, int timeInSeconds,
			RoutingTable routingTable) {
	
		this.lastChanged = CommonState.getTime();
		// is room left?
		if (entryCount < KademliaCommonConfig.K) {
			// does peer already exist?
			for (int i = 0; i < entryCount; i++) {
				IPeer p = peers[i];
				if (p.getId() == peerToAdd.getId()) {
					// update only if we already received a response from peer
					if (lastReceived[i] >= 0) {
						
						updatePeer(i);
					}
					return true;
				}
			}
			
			peers[entryCount] = peerToAdd;

			lastReceived[entryCount] = timeInSeconds;
			entryCount++;
			routingTable.incPeerNumber();
			return true;
		} else {

			// try to replace BAD peer
			for (int i = 0; i < entryCount; i++) {
				if (isBad(i)) {
					return replacePeer(i, peerToAdd, timeInSeconds);
				}
			}

			// try to replace QUESTIONABLE peer (by pinging the peer)
			if (pingPending)
				return false;
			
			for (int i = 0; i < entryCount; i++) {
				if (isQuestionable(i)) {
					
					// test if peer responds to ping
					IPeer p = peers[i];
					
					// " sends a ping to " + p.getId() + ", peer to add: " +
					// peer.getId());

					if (CommunicatorConfig.FAST_LOCAL_PINGS && p instanceof RemotePeer) {
						IPeer innerPeer = ((RemotePeer) p).getInnerPeer();
						

						if (innerPeer instanceof KademliaProtocol) {
							KademliaProtocol localKademliaProtocol = routingTable
									.getProtocol();
							KademliaProtocol remoteKademliaProtocol = (KademliaProtocol)innerPeer;

							
							if (!localKademliaProtocol.fastPing(remoteKademliaProtocol)) {
								
								
								
								return replacePeer(i, peerToAdd, timeInSeconds);
							}
						} else { // peer is offline
							return replacePeer(i, peerToAdd, timeInSeconds);
						}
					} else {
						
						// Logger.log(routingTable.getOwnId() + ": pinging " + p.getId() + ", wanting to add peer " + peerToAdd.getId());
						// peer is a RemoteRankPeer
						routingTable.getProtocol().startPing(p, peerToAdd,
								timeInSeconds);
						pingPending = true;
						return false;
					}
				}
			}

			// else discard peer
			return false;
		}
	}

	private boolean replacePeer(IPeer toReplace, int peerToAddId,
			int timeInSeconds) {
		Node n = Network.getById(peerToAddId);
		IPeer peerToAdd = null;
		if (n != null)
			peerToAdd = (IPeer) n.getProtocol(KademliaProtocol.kademliaid);
		else {
			peerToAdd = Communicator.getRemoteRankPeer(peerToAddId);
			if (peerToAdd == null) {
				return false;
			}
		}

		for (int i = 0; i < entryCount; i++) {
			if (peers[i].getId() == toReplace.getId()) {
				this.lastChanged = CommonState.getTime();
				
				

				peers[i] = peerToAdd;
				lastReceived[i] = timeInSeconds;
				return true;
			}
		}
		return false;
	}

	private boolean replacePeer(int i, IPeer peer, int timeInSeconds) {
		
		this.lastChanged = CommonState.getTime();
		peers[i] = peer;
		lastReceived[i] = timeInSeconds;
		return true;
	}

	/**
	 * Tries to remove a peer from this KBucket. If a matching peer is found it
	 * will be removed and <code>true</code> will be returned, else
	 * <code>false</code>.
	 * 
	 * @param peer
	 */
	public boolean removePeer(IPeer peer) {
		// Statistics.incNoEvents("trying to remove a peer");
		for (int i = 0; i < entryCount; i++) {
			IPeer n = peers[i];
			if (n.getId() == peer.getId()) {
				// Statistics.incNoEvents("removed a peer");
				removePeer(i);

				return true;
			}
		}
		// Statistics.incNoEvents("did not remove a peer");

		return false;
	}

	public void movePeerToBucket(int i, KBucket bucket) {
		bucket.peers[bucket.entryCount] = this.peers[i];
		bucket.lastReceived[bucket.entryCount] = this.lastReceived[i];
		bucket.entryCount++;
		removePeer(i);
	}

	private void removePeer(int index) {

		int numMoved = entryCount - index - 1;
		if (numMoved > 0) {
			System.arraycopy(peers, index + 1, peers, index, numMoved);
			System.arraycopy(lastReceived, index + 1, lastReceived, index,
					numMoved);
		}
		entryCount--;
		// Let gc do its work
		peers[entryCount] = null;
	}

	public Object clone() {
		KBucket dolly = new KBucket();
		System.arraycopy(this.peers, 0, dolly.peers, 0, this.entryCount);
		System.arraycopy(this.lastReceived, 0, dolly.lastReceived, 0,
				this.entryCount);
		return dolly;
	}

	private String getModeString(int i) {
		if (isGood(i))
			return "G";
		else if (isQuestionable(i))
			return "Q";
		else if (isBad(i))
			return "B";
		else
			return "Q";
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		for (int i = 0; i < entryCount; i++) {
			IPeer peer = peers[i];
			sb.append(String.format("%31s %s \n", Util.toBinaryString(peer
					.getId()), getModeString(i)));
		}
		sb.append("}\n");
		return sb.toString();
	}

	public long getLastChanged() {
		return lastChanged;
	}

	public boolean isFull() {
		return entryCount >= KademliaCommonConfig.K;
	}

	/**
	 * Adds all nodes within this bucket to the specified list.
	 * 
	 * @param list
	 */
	public void addNodesToList(List<IPeer> list) {
		for (int i = 0; i < entryCount; i++) {
			list.add(peers[i]);
		}
	}

	public int size() {
		return entryCount;
	}

	public IPeer getPeer(int j) {
		return peers[j];
	}

	public void onIncomingPingTimeout(IPeer sourcePeer, int peerToAddId,
			int lastReceivedFromPeerToAdd, RoutingTable routingTable) {
		pingPending = false;

		/* Logger.log(routingTable.getOwnId() + ": there is an actual ping timeout, replacing peer " +
		sourcePeer.getId() + " with " + peerToAddId); */
		replacePeer(sourcePeer, peerToAddId, lastReceivedFromPeerToAdd);
	}

	public void onIncomingPingResponse(IPeer sourcePeer, int peerToAddId,
			int lastReceived, RoutingTable routingTable) {
		pingPending = false;

		/* Logger.log(routingTable.getOwnId() + " there is an actual ping response, not replacing peer " +
		 sourcePeer.getId() + " with " + peerToAddId); */

		notifyAboutIncResponse(sourcePeer, routingTable);

		// retry adding the new peer
		addPeer(peerToAddId, CommonState.getKademliaSecTime(), routingTable);
	}

}
