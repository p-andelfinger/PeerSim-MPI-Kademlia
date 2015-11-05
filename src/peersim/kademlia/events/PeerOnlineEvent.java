package peersim.kademlia.events;

import peersim.core.Node;
import peersim.kademlia.PeerCreatorNode;
import peersim.kademlia.StateBuilder.Location;

/**
 * A new peer should be created. This event must <b>only</b> be thrown to a
 * {@link PeerCreatorNode}. This is a hack, as events can only be thrown to
 * {@link Node}s but the peer to create does not exist yet. The peer will be
 * created at {@link Location} {@link #getLocation()} and live for
 * {@link #getSessionLength()} time units. 
 * 
 * @author Konrad
 * 
 */
public class PeerOnlineEvent extends KademliaEvent {
	
	private Location location;
	private long sessionLength;

	public PeerOnlineEvent(Location location, long sessionLength) {
		super();
		this.location = location;
		this.sessionLength = sessionLength;
	}
	
	public Location getLocation() {
		return this.location;
	}

	public long getSessionLength() {
		return this.sessionLength;
	}
	
	@Override
	public void clear() {
		this.location = null;
	}
}
