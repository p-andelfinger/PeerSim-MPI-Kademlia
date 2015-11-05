package peersim.kademlia.events;

import peersim.core.CommonState;

public abstract class KademliaEvent {

	/**
	 * The identifier of the type of the event.
	 */
	protected int type;

	public long timestamp;

	public KademliaEvent() {
		this.timestamp = CommonState.getTime();
	}

	/**
	 * Initializes the type of the event.
	 * 
	 * @param type
	 *            The identifier of the type of the event
	 */
	public KademliaEvent(int type) {
		this();
		this.type = type;
	}

	/**
	 * Gets the type of the event.
	 * 
	 * @return The type of the current event.
	 */
	public int getType() {
		return this.type;
	}

	/**
	 * Sets all object references of this event to <code>null</code>, to dispose of objects.
	 */
	public void clear() {
		
	}
	
}