package peersim.kademlia;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import peersim.MPI.Communicator;
import peersim.MPI.CommunicatorConfig;
import peersim.MPI.Statistics;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.ExtendedRandom;

/**
 * Some utility and mathematical function to work with BigInteger numbers and strings.
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class Util {

	
	private static final double MINIMUM_DISTANCE = Math.pow(2, 129);
	
	private static Lock bootstrapLock = new ReentrantLock();

	public static void main(String[] args) {
		
		ExtendedRandom r = new ExtendedRandom(100);
		int myself = r.nextInt(Integer.MAX_VALUE);
				
		BigInteger myselfBI = BigInteger.valueOf(myself);
		
		for (int i = 0; i < 1000; i++) {
			int l = r.nextInt(Integer.MAX_VALUE);
			BigInteger bi = BigInteger.valueOf(l);
			
			
			int len1 = prefixLen(myself, l);
//			int len2 = prefixLen(myselfBI, bi)-129;
//			if (len1 != len2)
//				System.out.println("ERROR!");
				System.out.println(String.format("%31s", Integer.toString(myself, 2)));
				System.out.println(String.format("%31s", Integer.toString(l, 2)));
				System.out.println(len1);
//				System.out.println(len2);
				System.out.println(realDistance(myself, l));
				System.out.println("-------------");
		}
	}
	
//	/**
//	 * Given two numbers, returns the length of the common prefix, i.e. how many digits (in base 2) have in common from the
//	 * leftmost side of the number
//	 * 
//	 * @param id1
//	 * @param id2
//	 * @return int
//	 */
//	public static final int prefixLen(long id1, long id2) {
//		long temp = (id1 ^ id2);
//		return Long.numberOfLeadingZeros(temp)-1; 
//	}

	/**
	 * Returns the {@link RemotePeer} of the {@link Node} currently at index
	 * <code>i</code> within the {@link Network} or <code>null</code>, if there
	 * is no node at position <code>i</code>, or the node does not have
	 * {@link RemotePeer} at position {@link KademliaProtocol#kademliaid}.
	 * 
	 * @return
	 */
	public static RemotePeer getPeer(int i) {
		Node n = Network.get(i);
		if (n == null)
			return null;
		try {
			RemotePeer peer = (RemotePeer) n
					.getProtocol(KademliaProtocol.kademliaid);
			return peer;
		} catch (Exception e) {
			return null;
		}
	}
	
	/**
	 * Chooses a random peer that is online.
	 * @return
	 * @see RemotePeer#isOnline()
	 */
	
	// static Set<IPeer> uniquePeers = new HashSet<IPeer>();
	
	public static RemotePeer chooseRandomOnlinePeer() {
		// choose random peer that is online
		while (true) {
			Node n = Network.get(CommonState.r.nextInt(Network.size()));
			if (!(n instanceof PeerCreatorNode)) {
				RemotePeer peer = (RemotePeer) n.getProtocol(KademliaProtocol.kademliaid);
				
				if (peer.toNode() != null && peer.isOnline()) {
					// Statistics.incNoEvents("chosenPeers");
					/* if(uniquePeers.add(peer)) {
						Statistics.incNoEvents("uniqueChosenPeers");
					} */
					return peer;
				}
			}
		}
	}
	
	/**
	 * Chooses a random peer that is online from peers simulated on any rank.
	 * @return
	 * @see RemotePeer#isOnline()
	 */
	public static IPeer chooseRandomOnlinePeerGlobally() {
		//return chooseRandomOnlinePeer();
		// Communicator.log("getting new random online peer");
		// choose random peer that is online
		
		int targetRank = CommonState.r.nextInt(Communicator.getSize());
		if(targetRank == Communicator.getMyRank()) {
			// Statistics.incNoEvents("bootstrapPeerFromMyRank");
			return chooseRandomOnlinePeer();
		}
		// Statistics.incNoEvents(Communicator.getMyRank() + " would use bootstrap peer from " + targetRank);
		
		return Communicator.getLastReceivedRandomOnlinePeer(targetRank);
		

	}

	/**
	 * Chooses a random ID of the kademlia ID space. That is, a random integer
	 * between 0 and {@link Integer#MAX_VALUE}, inclusively.
	 * 
	 * @return
	 */
	public static int chooseRandomPeerId() {
		
		if(CommunicatorConfig.ID_BASED_PARTITIONING) {
			int rangeSize = Integer.MAX_VALUE / Communicator.getSize(); // the size of id range covered by this rank
			
			int base = Communicator.getMyRank() * rangeSize;
			int offset = CommonState.r.nextInt(rangeSize);
			
			return base + offset;
		} else {
			return CommonState.r.nextInt(Integer.MAX_VALUE);
		}
	}

	public static final int prefixLen(int id1, int id2) {
		int temp = (id1 ^ id2);
		return Integer.numberOfLeadingZeros(temp)-1; 
	}
	
//	/**
//	 * Given two numbers, returns the length of the common prefix, i.e. how many digits (in base 2) have in common from the
//	 * leftmost side of the number
//	 * 
//	 * @param b1
//	 *            BigInteger
//	 * @param b2
//	 *            BigInteger
//	 * @return int
//	 */
//	public static final int prefixLen(BigInteger b1, BigInteger b2) {
//		// if b1 and b2 do not have the same length, we already now the prefix length
//		int b1L = b1.bitLength();
//		int b2L = b2.bitLength();
//		int diff = b1L - b2L;
//		if (diff == 0) {
//			for (int i = b1L - 1; i >= 0; i--) {
//				if (b1.testBit(i) != b2.testBit(i)) {
//					return KademliaCommonConfig.BITS - (i + 1);
//				}
//			}
//			return KademliaCommonConfig.BITS;
//		} else if (diff > 0) { 
//			return KademliaCommonConfig.BITS - b1L;
//		} else { // diff < 0
//			return KademliaCommonConfig.BITS - b2L;
//		}
//	}
//	/**
//	 * Given two numbers, returns the length of the common prefix, i.e. how many digits (in base 2) have in common from the
//	 * leftmost side of the number
//	 * 
//	 * @param b1
//	 *            BigInteger
//	 * @param b2
//	 *            BigInteger
//	 * @return int
//	 */
//	public static final int prefixLen(BigInteger b1, BigInteger b2) {
//
//
//		String s1 = Util.put0(b1);
//		String s2 = Util.put0(b2);
//
//		int i = 0;
//		for (i = 0; i < s1.length(); i++) {
//			if (s1.charAt(i) != s2.charAt(i))
//				return i;
//		}
//
//		return i;
//	}

//	/**
//	 * return the distance between two number which is defined as (a XOR b)
//	 * 
//	 * @param a
//	 *            BigInteger
//	 * @param b
//	 *            BigInteger
//	 * @return BigInteger
//	 */
//	public static final BigInteger distance(BigInteger a, BigInteger b) {
//		return a.xor(b);
//	}

	
	/**
	 * Return the distance between two numbers which is defined as (a XOR b).
	 * @param a
	 * @param b
	 * @return
	 */
	public static final int distance(int a, int b) {
		return a ^ b;
	}
	
	/**
	 * Return the distance between two numbers which is defined as (a XOR b). Both integers will be interpreted as if they
	 * were just the first 31 bits of a 160 bit array and all other bits were set to 0.  
	 * @param a
	 * @param b
	 * @return
	 */
	public static final double realDistance(int a, int b) {
		return MINIMUM_DISTANCE * ((double) (a ^ b));
	}
	
	public static String toBinaryString(int id) {
		return String.format("%31s", Integer.toBinaryString(id)).replace(' ', '0');
	}
	public static String toBinaryString(int id, int stringLength) {
		return String.format("%" + stringLength + "s", Integer.toBinaryString(id)).replace(' ', '0');
	}
	
//	/**
//	 * convert a BigInteger into a String (base 2) and lead all needed non-significative zeroes in order to reach the canonical
//	 * length of a nodeid
//	 * 
//	 * @param b
//	 *            BigInteger
//	 * @return String
//	 */
//	private static final String put0(BigInteger b) {
//		if (b == null)
//			return null;
//		String s = b.toString(2); // base 2
//		while (s.length() < KademliaCommonConfig.BITS) {
//			s = "0" + s;
//		}
//		return s;
//	}

}
