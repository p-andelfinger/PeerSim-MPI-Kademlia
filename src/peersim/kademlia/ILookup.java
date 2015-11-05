package peersim.kademlia;

import peersim.kademlia.events.FindNodeEvent;

public interface ILookup {
	public int getLookupId();
	
	public boolean hasFinished();
	
	public void onTimeout(FindNodeEvent evt, int pId);
	
	public void onResponse(FindNodeEvent evt, int pId);
}
