package peersim.kademlia;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.MPI.Statistics;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.StateBuilder.Location;
import peersim.kademlia.events.FindNodeEvent;
import peersim.kademlia.events.KademliaEvent;
import peersim.kademlia.events.PingEvent;

public class RemotePeer implements Cloneable, EDProtocol, IPeer {

	public static final String PAR_K = "K";
	public static final String PAR_ALPHA = "ALPHA";
	public static final String PAR_BITS = "BITS";
	public static final String PAR_TIMEOUT = "TO";
	public static final String PAR_PLOT = "PLOT_LOOKUPS";

	private static final OfflinePeer OFFLINE_PEER = new OfflinePeer();

	private static String prefix = null;
	
	private static boolean wasLoaded = false;

	/** the ID of this nodes */
	private int nId;

	/**
	 * the inner peer, which might be either a {@link KademliaProtocol} or the
	 * {@link OfflinePeer} if the peer is considered being offline.
	 */
	private IPeer peer;

	/** the {@link Node} this peer links to */
	private Node node;
	
	public IPeer getInnerPeer() {
		return peer;
	}

	/**
	 * Used only by the initializer when creating the prototype. Every other
	 * instance call CLONE to create the new object.
	 * 
	 * @param prefix
	 *            String
	 */
	public RemotePeer(String prefix) {
		if(!wasLoaded) {
			KademliaCommonConfig.loadKademliaConfig(prefix);
		}
		
		/* // read parameters
		if (RemotePeer.prefix == null) {
			RemotePeer.prefix = prefix;
			KademliaCommonConfig.K = Configuration.getInt(prefix + "." + PAR_K,
					KademliaCommonConfig.K);
			KademliaCommonConfig.ALPHA = Configuration.getInt(prefix + "."
					+ PAR_ALPHA, KademliaCommonConfig.ALPHA);
			KademliaCommonConfig.TIMEOUT_DURATION = Configuration.getInt(prefix
					+ "." + PAR_TIMEOUT, KademliaCommonConfig.TIMEOUT_DURATION);
			KademliaCommonConfig.PLOT_LOOKUPS = Configuration.getBoolean(prefix
					+ "." + PAR_PLOT, KademliaCommonConfig.PLOT_LOOKUPS);
		} */

		this.nId = -1; // empty nodeId until init(int) is called
	}

	/**
	 * Used for cloning.
	 */
	public RemotePeer() {
		this.nId = -1; // empty nodeId until init(int) is called
	}

	public void init(Node node, int id, long sessionEnd) {
		this.node = node;
		this.nId = id;
		KademliaProtocol prot = new KademliaProtocol(this);
		prot.setSessionEnd(sessionEnd);
		this.peer = prot;


		prot.generateNextRoutingTableCheck(true);
	}

	@Override
	public void setLocation(Location loc) {
		peer.setLocation(loc);
	}

	@Override
	public Location getLocation() {
		return peer.getLocation();
	}

	@Override
	public RemotePeer clone() {
		RemotePeer dolly = new RemotePeer();
		return dolly;
	}

	@Override
	public void processEvent(Node node, int pId, Object event) {
		peer.processEvent(node, pId, event);
		
		if (event instanceof KademliaEvent) {
			((KademliaEvent) event).clear();
		}
	}

	@Override
	public void bootstrap() {
		peer.bootstrap();
	}

	public int getId() {
		return nId;
	}

	/**
	 * Sets peer node to offline and removes it from the underlying network.
	 * Thus, no further events will be received by this peer. However, the peer
	 * might still remain in the routing tables of other peers for some time.
	 */
	public void shutdown() {
		this.peer = OFFLINE_PEER;

		// remove node from network
		Network.remove(node.getIndex());
	}

	public Node toNode() {
		return this.node;
	}

	public boolean canReceiveFrom(IPeer sender) {
		return peer.canReceiveFrom(sender);
	}

	@Override
	public boolean canReceiveFrom(IPeer sender, long atTime) {
		return peer.canReceiveFrom(sender, atTime);
	}
	
	public boolean isOnline() {
		return peer.isOnline();
	}

	@Override
	public String toString() {
		return Util.toBinaryString(nId);
	}

	public static final class OfflinePeer implements IPeer {

		public Node toNode() {
			return null;
		}
		
		public int getId() {
			return -1;
		}
		
		public OfflinePeer() {
			// do nothing
		}

		@Override
		public void setLocation(Location loc) {
			// do nothing
		}

		@Override
		public Location getLocation() {
			return null;
		}

		@Override
		public void bootstrap() {
			// do nothing
		}

		@Override
		public boolean canReceiveFrom(IPeer sender, long atTime) {
			return false;
		}
	
		
		@Override
		public boolean canReceiveFrom(IPeer sender) {
			return false;
		}


		@Override
		public boolean isOnline() {
			return false;
		}

		@Override
		public void processEvent(Node node, int pId, Object event) {
			
			if(event instanceof PingEvent) {
				PingEvent evt = (PingEvent) event;
				if(evt.getType() == PingEvent.EVT_PING) {
					RemotePeer peer = (RemotePeer) node.getProtocol(KademliaProtocol.kademliaid);
					PingEvent timeoutEvent = new PingEvent(PingEvent.EVT_PING_TMO, peer, evt.peerToAddId, evt.lastReceived);
					
					Communicator.scheduleEvent(KademliaCommonConfig.PING_TIMEOUT_DELAY, evt.source, timeoutEvent);
					//Statistics.incNoEvents("timeout on offline peer");
					
					/* if (evt.src instanceof RemoteRankPeer) {

						RemoteRankPeer rDest = (RemoteRankPeer) evt.src;
						Communicator.forwardEvent(KademliaCommonConfig.PING_TIMEOUT_DELAY, evt.src, timeoutEvent, rDest
								.getRank()); // to make it perfect, the latency of the
												// ping packet should be subtracted, but
												// it shouldn't matter much
					} else {
						
						EDSimulator.add(KademliaCommonConfig.PING_TIMEOUT_DELAY, timeoutEvent,
								evt.src.toNode(), KademliaProtocol.kademliaid);
					} */
				}
			}
			
			if (event instanceof FindNodeEvent) {
				FindNodeEvent evt = (FindNodeEvent) event;
				
				// if an offline peer receives a request, the sending peer must be informed about the timeout
				// (this can only happen if the receiving peer went offline after the packet was sent)
				if (evt.getType() == FindNodeEvent.EVT_RCV_FINDNODE_RQU) {
					
					RemotePeer peer = (RemotePeer) node
							.getProtocol(KademliaProtocol.kademliaid);
					FindNodeEvent timeoutEvent = FindNodeEvent.createFindNodeTimeoutEvent(evt.operation, peer);
					Communicator.scheduleEvent(KademliaCommonConfig.TIMEOUT_DURATION, evt.src, timeoutEvent);
					
					/* if(evt.src instanceof RemoteRankPeer) {
						Statistics.incNoEvents("creating timeout because peer is offline: remote");

						RemoteRankPeer rPeer = (RemoteRankPeer) evt.src;
						
						// forward timeout to source remote rank
						FindNodeEvent timeoutEvent = FindNodeEvent.createFindNodeTimeoutEvent(evt.operation, peer);
						
						Communicator.forwardEvent(KademliaCommonConfig.TIMEOUT_DURATION, evt.src, timeoutEvent, rPeer.getRank()); // should probably subtract the incoming packet's latency here, but shouldn't matter much
						

					} else {
						Statistics.incNoEvents("creating timeout because peer is offline: local");

						FindNodeEvent timeoutEvent = FindNodeEvent.createFindNodeTimeoutEvent(evt.operation, peer);
						
						//peer.scheduleEvent(KademliaCommonConfig.TIMEOUT_DURATION, evt.src, timeoutEvent);
						
						EDSimulator.add(KademliaCommonConfig.TIMEOUT_DURATION, timeoutEvent,
										evt.src.toNode(), KademliaProtocol.kademliaid);
						
					} */
					

				} else if (evt.getType() == FindNodeEvent.EVT_RCV_FINDNODE_RSP) {
				} else if (evt.getType() == FindNodeEvent.EVT_RCV_FINDNODE_TMO) {
				}
			}
			// do nothing
		}

	}




}
