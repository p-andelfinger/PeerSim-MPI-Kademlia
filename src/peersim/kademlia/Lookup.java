package peersim.kademlia;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import peersim.MPI.Logger;
import peersim.MPI.Statistics;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.kademlia.events.FindNodeEvent;
import edu.kit.tm.kademliaPlot.KPlot;
import edu.kit.tm.kademliaPlot.KademliaPlot;

public class Lookup implements ILookup {
	public static long OPERATION_ID_GENERATOR = 0;
	public static long DESTRUCTOR_COUNTER = 0;
	public static long LOOKUPS_FINISHED = 0;
	public static long LOOKUPS_CANCELED = 0;
	public static long LOOKUPS_LOST_PACKETS = 0;
	public static long NOT_EVEN_STARTED = 0;
	
	private static int nextLookupId = 0;

	/**
	 * represent uniquely the find operation
	 */
	public long operationId;

	/**
	 * Id of the node to find
	 */
	private int targetId;

	/** number of sent but yet unanswered requests */
	public int pendingRequests = 0;

	/**
	 * Start timestamp of the search operation
	 */
	protected long timestamp = 0;

	/**
	 * Number of messages sent by this findOperation
	 */
	private int messagesSent = 0;

	private LinkedList<PeerEntry> closestSet;
	
	private KademliaProtocol parent;

	private boolean isDone = false;
	
	private boolean plotLookup;
	private KPlot plot = null;
	
	private int lookupId;

	public Lookup(KademliaProtocol parent, int destId, long timestamp) {
		this.parent = parent;
		this.targetId = destId;
		this.timestamp = timestamp;
		
		// set a new find id
		operationId = OPERATION_ID_GENERATOR++;

		// initialize closestSet
		closestSet = new LinkedList<Lookup.PeerEntry>();
		
		this.lookupId = Lookup.getNextLookupId();
		
	}
	

	public static int getNextLookupId() {
		return nextLookupId++;
	}
	
	public int getTargetId() {
		return targetId;
	}
	
	@Override
	protected void finalize() throws Throwable {
		DESTRUCTOR_COUNTER++;
	}

	private PeerEntry getNodeEntry(IPeer peer) {
		for (PeerEntry ne : closestSet) {
			if (ne.peer.getId() == peer.getId())
				return ne;
		}
		
		return null;
	}
	
	public int getLookupId() {
		return lookupId;
	}
	
	public void onStart(int pId) {
		// add nodes closest to targetId to closest nodes set 
		List<IPeer> neighbors = parent.routingTable.getClosestPeers(targetId);
		addNodes(neighbors, 0);
		if (neighbors.size() == 0) { 
			//Logger.log("node " + parent.getId() + " got no neighbours, so cannot start lookup");
			Lookup.NOT_EVEN_STARTED++;
		}
			
		// send first ALPHA requests
		
		continueQuerying(pId);
		
		// plot
		plotOnStart(neighbors);
		
		checkIfFinished();
	}

	public void onTimeout(FindNodeEvent evt, int pId) {
		//Statistics.incNoEvents("onTimeout");
		
		PeerEntry ne = getNodeEntry(evt.src);
		
		if(ne == null) {
			//Statistics.incNoEvents("node entry not found: onTimeout");
			// in the parallel case, this can happen if duplicate ids exist on multiple ranks
			return;
		}

		// rpc completed
		KademliaObserver.timeoutfindNode++;
		ne.setTimedOut();
		pendingRequests--;
		
		
		continueQuerying(pId);
		
		checkIfFinished();
	}

	public void onResponse(FindNodeEvent evt, int myPid) {
		//Statistics.incNoEvents("onResponse");
		
		// get Node Entry for source node
		PeerEntry ne = getNodeEntry(evt.src);
		
		if(ne == null) {
			// in the parallel case, this can happen if an id exists on multiple ranks
			return;
		}
		
		// rpc completed
		ne.setResponded();
		pendingRequests--;


		// get nodes from message body
		@SuppressWarnings("unchecked")
		List<IPeer> peers = (List<IPeer>) evt.body;

		//Logger.log("number of peers in peerToAdd: " + peers.size());

		// save received neighbors in the closest nodes set
		addNodes(peers, evt.hops);

		continueQuerying(myPid);

		checkIfFinished();
	}

	public final boolean hasFinished() {
		return this.isDone;
	}

	private void checkIfFinished() {
		// do not call if already finished
		if (isDone)
			return;
		
		// finish if the first K nodes that did not time out yet have responded
		boolean allNodesResponded = false;
		int counter = 0;
		for (PeerEntry ne : closestSet) {
			if (!ne.wasQueried()) {
				// one of the first nodes has not been queried, so abort
				break;
			}
			if (ne.hasResponded()) {
				// node did respond, count him
				counter++;
				// are we done with this FindOperation?
				if (counter >= KademliaCommonConfig.K) {
					allNodesResponded = true;
					break;
				}
			} else if (ne.hasTimedOut()) {
				// node did time out, so skip him
				// do nothing
			} else {
				// node did neither time out, nor did he respond, so we have to wait. abort.
				break;
			}
		}
	
		// successfully finished! clean up
		if (allNodesResponded) {
			//Statistics.incNoEvents("lookup finished");
			finish(allNodesResponded);
		}
		
		// lookup aborted. clean up
		if (pendingRequests <= 0) {
			cancel();
		}
	}

	private void finish(boolean allNodesResponded) {
		this.isDone = true;
		LOOKUPS_FINISHED++;
		
		// update statistics
		long timeInterval = CommonState.getTime() - timestamp;
		KademliaObserver.timeStore.add(timeInterval);
		
		KademliaObserver.sumtimeStore.add(timeInterval);
		KademliaObserver.lookupCost.add(messagesSent);
		KademliaObserver.sumLookupCost.add(messagesSent);
		KademliaObserver.msg_deliv.add(1);
		
		plotOnFinish();

		cleanUp();
	}

	public void cancel() {
		
		// do not call if already finished
		if (isDone)
			return;
		
		isDone = true;
		LOOKUPS_CANCELED++;

		cleanUp();
	}
	
	private void cleanUp() {
		this.parent.removeLookup(this);
		this.parent = null;
		this.closestSet = null;
	}
	
	private void plotOnStart(List<IPeer> peers) {
		if (plotLookup) {
			long time = CommonState.getTime();
			KademliaPlot kp = KademliaPlot.getInstance();
			plot = kp.createNewPlot(time);
			plot.setSourceId(this.parent.getId());
			plot.setTargetId(this.targetId);

			plot.setTitle(String.format("PeerSim Lookup, %d peers online (parall.: %d, timeout: %.1fs)",
							Network.size(), KademliaCommonConfig.ALPHA, 
							((float)KademliaCommonConfig.TIMEOUT_DURATION) / 1000.0));
			Calendar cal = Calendar.getInstance();
			String fileName = String.format("PeerSimLookup_%tF_%tH-%tM-%tS.eps",
					cal, cal, cal, cal, cal);
//			String fileName = "PeerSimLookup.eps";
			plot.setFileName(fileName);
			plot.setFilePath("." + File.separator);
			
			for (IPeer rp : peers) {
				double distance = Util.realDistance(rp.getId(), targetId);
				plot.addNewPeer(time, distance, rp);
			}
		}
	}

	private void plotOnResponse(FindNodeEvent evt, List<IPeer> peers) {
		if (plot != null) {
			IPeer pp = evt.src;
			plot.addResponse(evt.timestamp, CommonState.getTime(), pp);
			
			for (IPeer peer : peers) {
				double distance = Util.realDistance(peer.getId(), targetId);
				plot.addNewPeer(pp, distance, peer);
			}
		}
	}

	private void plotOnTimeout(FindNodeEvent evt) {
		if (plot != null) {
			plot.addTimeout(evt.timestamp, CommonState.getTime(), evt.src);
		}
	}

	private void plotOnFinish() {
			if (plot != null) {
	//			plot.setTitle("Lookup for target " + targetId);
				plot.setEndingTime(CommonState.getTime());
				int c = 0;
				for (PeerEntry ne : closestSet) {
					if (ne.hasResponded()) {
						// node did respond, count him
						c++;
						plot.addLookupResultPeer(ne.peer);
						// are we done?
						if (c >= KademliaCommonConfig.K) 
							break;
					}
				}
	
				plot.plot();
			}
		}

	private void addNodes(List<IPeer> peersToAdd, int hops) {
		//Statistics.incNoEvents("called addNodes");
		
		// add to closestSet
		for (IPeer p : peersToAdd) {
			//Statistics.incNoEvents("addNodesPeersTried");
			// Communicator.log("find node response, adding node: " + p.getId());
			
			// do not process null nodes
			if (p == null) {
				//Statistics.incNoEvents("addNodesPeerNull");

				continue;
			}
			
			// do not process myself
			if (p.getId() == parent.getRemotePeer().getId()) {
				//Statistics.incNoEvents("addNodesIsMe");

				continue;
			}
			
			// compute distance from targetID
			int dist = Util.distance(p.getId(), targetId);

			// insert at correct position
			boolean isAlreadyInList = false;
			for (ListIterator<PeerEntry> it = closestSet.listIterator(); it.hasNext();) {
				PeerEntry cur = it.next();
				
				if (cur.dist >= dist) {
					if (cur.dist > dist) {
						it.previous();
						it.add(new PeerEntry(p, hops, dist));
						isAlreadyInList = true;
						//Statistics.incNoEvents("added a peer to closestNodes");
						break;
					} else {
						// cur.dist == dist, so element is already contained by list -> do not add.
						//Statistics.incNoEvents("addNodesIsAlreadyInList");
						isAlreadyInList = true;
						break;
					}
				}
			}
			if (!isAlreadyInList) {

				closestSet.addLast(new PeerEntry(p, hops, dist));
			}
		}
	}

	private void continueQuerying(int pId) {

		// send at maximum ALPHA messages
		int requestsToSend = KademliaCommonConfig.ALPHA - pendingRequests;

		// get nodes to query
		ArrayList<Lookup.PeerEntry> toQuery = new ArrayList<Lookup.PeerEntry>(requestsToSend);
		for (PeerEntry ne : closestSet) {
			if (!ne.wasQueried()) {
				toQuery.add(ne);
				requestsToSend--;
				if (requestsToSend == 0) 
					break;
			}
		}
		
		
		// query nodes
		for (PeerEntry ne : toQuery) {
			//Logger.log("lookup " + operationId + " sending request to " + ne.peer.getId());
			sendRequest(ne, pId);
		}
	}
	
	private void sendRequest(PeerEntry destNode, int pId) {
		messagesSent++;
		
		pendingRequests++;

		
		KademliaObserver.findNodequeries++;
		destNode.setQueried();

		parent.sendFindNodeRequest(destNode.peer, pId, this, destNode.hops + 1);
	}
	
	
	public static class PeerEntry {
		private static final short FLAG_QUERIED = 1;
		private static final short FLAG_RESPONDED = 2;
		private static final short FLAG_TIMED_OUT = 4;
		
		private IPeer peer;
		
		private int hops;
		
		private short flags; // save flags in a single short to conserve memory
		
		private int dist;

		public PeerEntry(IPeer p, int hops, int distance) {
			this.peer = p;
			this.hops = hops;
			this.dist = distance;
			this.flags = 0;
		}

		public void setQueried() {
			setBit(FLAG_QUERIED);
		}

		public void setResponded() {
			setBit(FLAG_RESPONDED);
		}

		public void setTimedOut() {
			setBit(FLAG_TIMED_OUT);
		}

		
		public boolean hasTimedOut() {
			return isBitSet(FLAG_TIMED_OUT);
		}

		public boolean hasResponded() {
			return isBitSet(FLAG_RESPONDED);
		}

		public boolean wasQueried() {
			return isBitSet(FLAG_QUERIED);
		}
		
		private void setBit(short mask) {
			flags = (short) (flags | mask);
		}
		
//		private void unsetBit(short mask) {
//			flags = (short) (flags & ~mask);
//		}
		
		private boolean isBitSet(short mask) {
			return (flags & mask) != 0;
		}
		
		@Override
		public String toString() {
			return dist + " " + (wasQueried() ? "Q" : "-") + (hasResponded() ? "R" : "-") + (hasTimedOut() ? "T" : "-");
		}
	}


}
