package peersim.kademlia.events;

import java.util.List;

import peersim.kademlia.ILookup;
import peersim.kademlia.IPeer;
import peersim.kademlia.Lookup;
import peersim.kademlia.RemotePeer;

public class FindNodeEvent extends KademliaEvent {

	/**
	 * The node should start a new Lookup.  
	 */
	public static final int EVT_SRT_FINDNODE = 2;

	/**
	 * The node just received an incoming findNode request. It should thus answer with a response. 
	 */
	public static final int EVT_RCV_FINDNODE_RQU = 3;

	/**
	 * The node just received a response to a previously sent findNode request. The node should thus continue with
	 * its corresponding FindOperation. 
	 */
	public static final int EVT_RCV_FINDNODE_RSP = 4;

	/**
	 * Timeout for a previously started findNode request.  
	 */
	public static final int EVT_RCV_FINDNODE_TMO = 7;
	
	/**
	 * This Object contains the body of the message, no matter what it contains
	 */
	public Object body = null;

	public ILookup operation;

	/**
	 * the target id we are looking for
	 */
	public int target;

	/**
	 * Source peer of this event. 
	 */
	public IPeer src;

	/**
	 * Available to count the number of hops the message did.
	 */
	public int hops = 0;

	private boolean plotLookup = false;
	
	public static FindNodeEvent createFindNodeRequestTimeout(ILookup op, IPeer source, int hops) {
		FindNodeEvent evt = new FindNodeEvent(FindNodeEvent.EVT_RCV_FINDNODE_TMO);
		evt.operation = op;
		evt.src = source;
		evt.hops = hops;
		
		return evt;
	}
	
	public static FindNodeEvent createFindNodeRequest(Lookup op, RemotePeer source) {
		FindNodeEvent evt = new FindNodeEvent(FindNodeEvent.EVT_RCV_FINDNODE_RQU);
		evt.operation = op;
		evt.src = source;
		evt.target = op.getTargetId();
		
		return evt;
	}

	public static FindNodeEvent createFindNodeResponse(ILookup op, RemotePeer source, List<IPeer> peers, int hops, long requestSentAt) {
		FindNodeEvent evt = new FindNodeEvent(FindNodeEvent.EVT_RCV_FINDNODE_RSP);
		evt.operation = op;
		evt.src = source;
		evt.body = peers;
		evt.hops = hops;
		evt.timestamp = requestSentAt;
		
		return evt;
	}
	
	public static FindNodeEvent createLookupStart(int target, boolean plotLookup) {
		FindNodeEvent evt = new FindNodeEvent(EVT_SRT_FINDNODE, null);
		evt.target = target;
		evt.plotLookup = plotLookup;
		return evt;
	}

	/**
	 * Create a message with specific type and empty body
	 * 
	 * @param messageType
	 *            int type of the message
	 */
	public FindNodeEvent(int messageType) {
		this(messageType, "");
	}

	/**
	 * Creates a message with specific type and body
	 * 
	 * @param messageType
	 *            int type of the message
	 * @param body
	 *            Object body to assign (shallow copy)
	 */
	private FindNodeEvent(int messageType, Object body) {
		super(messageType);
		this.body = body;
	}

	public String toString() {
		return String.format("%20s from %10s", messageTypetoString(), src != null ? src.toString() : "-");
	}

	public FindNodeEvent copy() {
		FindNodeEvent dolly = new FindNodeEvent(this.type);
		dolly.src = this.src;
		dolly.target = this.target;
		dolly.operation = this.operation;
		dolly.body = this.body; // deep cloning?

		return dolly;
	}

	public String messageTypetoString() {
		switch (type) {
			case EVT_SRT_FINDNODE:
				return "EVT_SRT_FINDNODE";
			case EVT_RCV_FINDNODE_RQU:
				return "EVT_RCV_FINDNODE_RQU";
			case EVT_RCV_FINDNODE_RSP:
				return "EVT_RCV_FINDNODE_RSP";
			case EVT_RCV_FINDNODE_TMO:
				return "EVT_RCV_FINDNODE_TMO";
			default:
				return "UNKNOW:" + type;
		}
	}

	public boolean getPlotLookup() {
		return plotLookup;
	}
	
	@Override
	public void clear() {
		super.clear();
		this.body = null;
		this.operation = null;
		this.src = null;
	}

	public static FindNodeEvent createFindNodeTimeoutEvent(ILookup op, IPeer src) {
		FindNodeEvent timeoutEvent = new FindNodeEvent(EVT_RCV_FINDNODE_TMO);
		timeoutEvent.src = src;
		timeoutEvent.operation = op;
		return timeoutEvent;
	}
}
