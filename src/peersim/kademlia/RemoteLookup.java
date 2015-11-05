package peersim.kademlia;

import peersim.kademlia.events.FindNodeEvent;

public class RemoteLookup implements ILookup {

	int lookupId;
	
	public RemoteLookup(int id) {
		this.lookupId = id;
	}
	
	@Override
	public int getLookupId() {
		return lookupId;
	}

	@Override
	public boolean hasFinished() {
		// we don't know, so return false
		return false;
	}

	@Override
	public void onTimeout(FindNodeEvent evt, int pId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onResponse(FindNodeEvent evt, int pId) {
		// TODO Auto-generated method stub
		
	}

}
