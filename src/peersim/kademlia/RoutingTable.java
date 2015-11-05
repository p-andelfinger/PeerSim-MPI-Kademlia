package peersim.kademlia;

import java.util.ArrayList;
import java.util.List;

import peersim.MPI.Logger;
import peersim.MPI.Statistics;
import peersim.core.CommonState;

/**
 * @author Konrad
 *
 */
public class RoutingTable implements Cloneable {
	/** the id of the kademlia node this routing table belongs to */
	private int ownId;
	
	/** the buckets of this routing table. Bucket number i will contain all nodes with a prefix length of i.*/
	private ArrayList<KBucket> buckets;
	
	/** total number of peers within the routing table */
	private int peerNumber = 0; 
	
	private KademliaProtocol protocol;

	/**
	 * instantiates a new empty routing table with the specified size
	 */
	public RoutingTable(int ownId, KademliaProtocol protocol) {
		this.protocol = protocol;
		resetBuckets(ownId);
	}

	public int getOwnId() {
		return this.ownId;
	}
	
	public int countPeers() {
		return peerNumber;
	}
	
	protected void incPeerNumber() {
		peerNumber++;
	}
	
	protected void decPeerNumber() {
		peerNumber--;
		if (peerNumber <= 0)
			if (peerNumber == 0)
				System.out.println("Peer " + Util.toBinaryString(ownId) + " starved. No more online peers in routing table.");
			else 
				System.out.println("ERROR!");
	}
	
	private void resetBuckets(int newOwnId) {
		this.ownId = newOwnId;
		resetBuckets();
	}
	
	private void resetBuckets() {
		buckets = new ArrayList<KBucket>();
		buckets.add(new KBucket());
		peerNumber = 0;
	}

	public ArrayList<KBucket> getBuckets() {
		return this.buckets;
	}

	/**
	 * Notifies the routing table about an incoming request from a remote peer.
	 * The routing table will be updated with the respective peer: either the
	 * peer might be added, a previously existing entry might be
	 * updated, or the peer might just be dropped. See the BEP 5 standard for
	 * details.
	 * 
	 * @param from
	 *            The peer the request was coming from.
	 */
	public void notifyAboutIncRequest(IPeer from) {
		
		// do not add ourself to our routing table
		if (ownId == from.getId())
			return;
		
		// get the kbucket number we want to insert into
		int prefix_len = Util.prefixLen(ownId, from.getId());
		
		// if bucket is not the last one, insert without splitting.
		if (prefix_len < buckets.size()-1) {
			KBucket bucket = buckets.get(prefix_len);
			bucket.notifyAboutIncRequest(from, this);
		// if it is the last bucket or a deeper bucket, split bucket.
		} else {
			insertAndSplit(prefix_len, from, false);
		} 
	}

	/**
	 * Notifies the routing table about an incoming response from a remote peer.
	 * The routing table will be updated with the respective peer: either the
	 * peer might be added, a previously existing entry might be
	 * updated, or the peer might just be dropped. See the BEP 5 standard for
	 * details.
	 * 
	 * @param from
	 *            The peer the response was coming from.
	 */
	public void notifyAboutIncResponse(IPeer from) {
		// do not add ourself to our routing table
		if (ownId == from.getId())
			return;
		
		// get the kbucket number we want to insert into
		int prefix_len = Util.prefixLen(ownId, from.getId());
		
		// if bucket is not the last one, insert without splitting.
		if (prefix_len < buckets.size()-1) {
			KBucket bucket = buckets.get(prefix_len);
			bucket.notifyAboutIncResponse(from, this);
		// if it is the last bucket or a deeper bucket, split bucket.
		} else {
			insertAndSplit(prefix_len, from, true);
		} 
	}
	

	/**
	 * BEP 5:
	 * When the bucket is full of good nodes, the new node is simply discarded.
	 * 
	 * If any nodes in the bucket are known to have become bad, then one is
	 * replaced by the new node. 
	 * 
	 * If there are any questionable nodes in the
	 * bucket have not been seen in the last 15 minutes, the least recently seen
	 * node is pinged. If the pinged node responds then the next least recently
	 * seen questionable node is pinged until one fails to respond or all of the
	 * nodes in the bucket are known to be good. 
	 * 
	 * If a node in the bucket fails
	 * to respond to a ping, it is suggested to try once more before discarding
	 * the node and replacing it with a new good node.
	 */
	private void insertAndSplit(int prefix_len, IPeer peer, boolean wasResponse) {
		// get from currently deepest bucket to bucket into which the peer belongs 
		int i = buckets.size() - 1;
		KBucket bucket = buckets.get(i);
		if (!bucket.isFull()) {
			// there is room left, so insert peer
			if (wasResponse)
				bucket.notifyAboutIncResponse(peer, this);
			else {
				bucket.notifyAboutIncRequest(peer, this);
			}
			return;
		} else {
			// there is no room left, so split bucket
			KBucket newBucket = new KBucket();
			buckets.add(newBucket);

			// relocate peers from old bucket to new one, if necessary
			for (int j = bucket.size() - 1; j >= 0; j--) {
				IPeer p = bucket.getPeer(j);
				int pl = Util.prefixLen(p.getId(), ownId);
				if (pl != i) { // pl will either be > i or = i, but never < i 
					// peer has to be moved to new bucket
					bucket.movePeerToBucket(j, newBucket);
				}
				// else peer has to stay on this bucket
			}
//			// relocate peers from old bucket to new one, if necessary
//			for (int j = 0; j < bucket.size(); j++) {
//				RemotePeer p = bucket.getPeer(j);
//				int pl = Util.prefixLen(p.getId(), ownId);
//				if (pl != i) {
//					// peer has to be moved to new bucket
//					bucket.movePeerToBucket(j, newBucket);
//					j--;
//				}
//				// else peer has to stay on this bucket
//			}
			
			
			// try again to insert peer
			if (wasResponse)
				this.notifyAboutIncResponse(peer);
			else {
				this.notifyAboutIncRequest(peer);
			}
			return;
		}

		// bucket number prefix_len was full, so drop node
		// -> do nothing here.
	}
	
	/**
	 * Remove peer from the routing table.
	 * @param peer
	 */
	public void removePeer(IPeer peer) {
		//Statistics.incNoEvents("removing a peer");
		
		// get the bucket this node should be within
		KBucket bucket = getCorrespondingBucket(peer.getId());
		
		// remove the node from the bucket
		if (bucket.removePeer(peer))
			decPeerNumber();
	}
	
	private KBucket getCorrespondingBucket(int from) {
		int prefix_len = Util.prefixLen(ownId, from);
		
		// retrieve the corresponding bucket
		KBucket bucket;
		if (prefix_len >= buckets.size()) {
			// node should be in last bucket if bucket does not yet exist
			bucket = buckets.get(buckets.size() - 1);
		} else {
			bucket = buckets.get(prefix_len);
		}
		return bucket;
	}

	/**
	 * Returns the {@link KademliaCommonConfig#K} peers closest to ID
	 * <code>key</code>.
	 * 
	 * @param key
	 * @return
	 */
	public List<IPeer> getClosestPeers(int key) {
		ArrayList<IPeer> peers = new ArrayList<IPeer>(
				KademliaCommonConfig.K);
		getClosestPeers(key, peers);
		return peers;
	}
	
	/**
	 * Returns the {@link KademliaCommonConfig#K} peers closest to ID
	 * <code>key</code>.
	 * 
	 * @param key
	 * @return
	 */
	public void getClosestPeers(int key, List<IPeer> listToAddPeersTo) {
		// get the bucket the peers should be within
		KBucket bucket = getCorrespondingBucket(key);

		// return the bucket if it is full
		if (bucket.isFull()) {
			bucket.addNodesToList(listToAddPeersTo);
			return;
//			}
		}
		
		// else, fill free "slots" with peers from deeper buckets
		int prefix_len = Util.prefixLen(ownId, key);
		if (listToAddPeersTo == null) 
			listToAddPeersTo = new ArrayList<IPeer>(KademliaCommonConfig.K);
		
		bucket.addNodesToList(listToAddPeersTo);
		for (int i = prefix_len+1; i < buckets.size(); i++) {
			KBucket kb = buckets.get(i);
			
			int spaceLeft = KademliaCommonConfig.K - listToAddPeersTo.size();
			int nodeCount = kb.size();
			int toAdd = Math.min(spaceLeft, nodeCount);
			for (int j = 0; j < toAdd; j++) {
				listToAddPeersTo.add(kb.getPeer(j));
			}
			if (nodeCount >= spaceLeft)
				return;
		}
		
		// if we still haven't found any peers, look within shallower buckets
		if (listToAddPeersTo.size() == 0) {
			int i = prefix_len;
			if (prefix_len >= buckets.size()) {
				// node should be in last bucket if bucket does not yet exist
				i = buckets.size() - 1;
			}
			for (; i > 0; i--) {
				KBucket kb = buckets.get(i);

				int spaceLeft = KademliaCommonConfig.K - listToAddPeersTo.size();
				int nodeCount = kb.size();
				int toAdd = Math.min(spaceLeft, nodeCount);
				for (int j = 0; j < toAdd; j++) {
					listToAddPeersTo.add(kb.getPeer(j));
				}
				
				if (nodeCount >= spaceLeft)
					return;
			}
		}
	 }

	public KademliaProtocol getProtocol() {
		return this.protocol;
	}

	public Object clone() {
		RoutingTable dolly = new RoutingTable(this.ownId, this.protocol);
		
		return dolly;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("ownId: %31s (%d)", Util.toBinaryString(ownId), ownId));
		sb.append("\n");
		for (int i = 0; i < buckets.size() - 1; i++) {
			KBucket bucket = buckets.get(i);
			sb.append("B ");
			sb.append(i);
			sb.append(" = ");
			int bucketPrefix = calcBucketPrefix(i);
			sb.append(Util.toBinaryString(bucketPrefix, i+1));
			sb.append("...");
			sb.append("\n");
			sb.append(bucket.toString());
		}
		if (buckets.size() > 0) {
			sb.append("B ");
			int i = buckets.size() - 1;
			sb.append(i);
			sb.append(" = ");
			if (i != 0) {
				int bucketPrefix = calcBucketPrefix(i-1) ^ 1;
				sb.append(Util.toBinaryString(bucketPrefix, Math.max(1, i)));
			}
			sb.append("...");
			sb.append("\n");
			sb.append(buckets.get(i).toString());
		}
		return sb.toString();
	}

	private int calcBucketPrefix(int i) {
		int result = ((ownId >> (30 - i)) ^ 1);
		return result;
	}

	/*
	 * Each bucket should maintain a "last changed" property to indicate how
	 * "fresh" the contents are.
	 * 
	 * When a node in a bucket is pinged and it responds, or a node is added
	 * to a bucket, or a node in a bucket is replaced with another node, the
	 * bucket's last changed property should be updated.
	 * 
	 * Buckets that have not been changed in 15 minutes should be
	 * "refreshed." This is done by picking a random ID in the range of the
	 * bucket and performing a find_nodes search on it.
	 * 
	 * Nodes that are able to receive queries from other nodes usually do
	 * not need to refresh buckets often. Nodes that are not able to receive
	 * queries from other nodes usually will need to refresh all buckets
	 * periodically to ensure there are good nodes in their table when the
	 * DHT is needed.
	 */
	public void checkBuckets() {
		
		long freshnessThreshold = CommonState.getTime() - KademliaCommonConfig.BUCKET_AGING_TIME;
		for (int i = 0; i < buckets.size(); i++) {
			KBucket bucket = buckets.get(i);
			
			if (bucket.getLastChanged() < freshnessThreshold) {
			
				// refresh bucket
				// BEP 5:
				/*
				 * Buckets that have not been changed in 15 minutes should be
				 * "refreshed." This is done by picking a random ID in the range
				 * of the bucket and performing a find_nodes search on it.
				 */
				// get random id from bucket range:
				int target = generateRandomIdFromBucket(i);

				//Logger.log("slcb");
				protocol.startLookup(target, KademliaProtocol.kademliaid);
			}
		}
		
	}

	/**
	 * Generates a random id from bucket <code>i</code>.
	 * @param i
	 * @return
	 */
	private int generateRandomIdFromBucket(int i) {
		// get random int...
		int rnd = CommonState.r.nextInt(Integer.MAX_VALUE);
		// ... then change first bits according to bucket
		int bits = i + 1;
		rnd = rnd >> bits;
		int mask = Integer.MAX_VALUE << (31 - bits); // "filters" the first few bits form ownId
		int target = ((ownId & mask) ^ (1 << (31 - bits))) | rnd;
		target = Math.abs(target);
		return target;
	}

	public void onIncomingPingTimeout(IPeer src, int peerToAddId, int lastReceived) {
		
		KBucket bucket = getCorrespondingBucket(src.getId());
		
		bucket.onIncomingPingTimeout(src, peerToAddId, lastReceived, this);
		
	}

	public void onIncomingPingResponse(IPeer src, int peerToAddId, int lastReceived) {
		KBucket bucket = getCorrespondingBucket(src.getId());
		bucket.onIncomingPingResponse(src, peerToAddId, lastReceived, this);
		
	}

} 
