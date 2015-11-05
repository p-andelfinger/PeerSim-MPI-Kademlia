package peersim.kademlia;

import java.util.Random;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.MPI.Statistics;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.events.FindNodeEvent;

/**
 * This control generates random search traffic from nodes to random destination node.
 */
public class TrafficGenerator implements Control {

	private final static String PAR_PROT = "protocol";
	private final int pid;

	public TrafficGenerator(String prefix) {
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
	}

	/**
	 * Starts a new lookup for a random target from a randomly chosen peer.
	 * 
	 * @return boolean
	 */
	public boolean execute() {
		Node start;
		do {
			start = Network.get(CommonState.r.nextInt(Network.size()));
		} while ((start == null) || (!start.isUp()) || (start instanceof PeerCreatorNode));

		// send message
		EDSimulator.add(0, generateFindNodeMessage(), start, pid);
		
		return false;
	}

	/**
	 * Generates a new start lookup event by selecting a random destination.
	 * 
	 * @return Message
	 */
	private FindNodeEvent generateFindNodeMessage() {
		// choose a random id
		int target = CommonState.r.nextInt(Integer.MAX_VALUE);
		FindNodeEvent evt = FindNodeEvent.createLookupStart(target, KademliaCommonConfig.PLOT_LOOKUPS);

		return evt;
	}
}
