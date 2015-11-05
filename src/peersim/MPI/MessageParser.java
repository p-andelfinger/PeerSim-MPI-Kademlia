package peersim.MPI;

import peersim.MPI.MessageHandler.FindNodeRequestHandler;
import peersim.MPI.MessageHandler.FindNodeResponseHandler;
import peersim.MPI.MessageHandler.FindNodeTimeoutHandler;
import peersim.MPI.MessageHandler.IMessageHandler;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.IPeer;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.Util;
import peersim.kademlia.events.PingEvent;

public class MessageParser {
	
	private static int randomOnlinePeersMessage[] = new int[CommunicatorConfig.RANDOM_ONLINE_PEERS_PER_MESSAGE + 1];
	private static IMessageHandler findNodeRequestHandler = new FindNodeRequestHandler();
	private static IMessageHandler findNodeResponseHandler = new FindNodeResponseHandler();
	private static IMessageHandler findNodeTimeoutHandler = new FindNodeTimeoutHandler();	
	private static IMessageHandler statisticsHandler = new Statistics();
	
	public static void parseMessage(float[] receiveBuffer, int source) {
		// can only be a statistics message for now, so handle it
		Logger.log("received float-statistics from " + source);
		statisticsHandler.handle(receiveBuffer, source);
	}
	
	public static void parseMessage(int[] receiveBuffer, int source) {
		switch (receiveBuffer[0]) {
		case CommunicatorConfig.REQUEST_RANDOM_ONLINE_PEER_MESSAGE_ID:
			requestRandomOnlinePeerMsg(source);
			break;
		case CommunicatorConfig.RESPONSE_RANDOM_ONLINE_PEER_MESSAGE_ID:
			responseRandomOnlinePeerMsg(receiveBuffer, source);
			break;
		case CommunicatorConfig.SIGNAL_BOOTSTRAPPING_COMPLETE:
			signalBootstrappingComplete(source);
			break;
		case CommunicatorConfig.FIND_NODE_REQUEST_EVENT:
			findNodeRequestHandler.handle(receiveBuffer, source);
			break;
		case CommunicatorConfig.FIND_NODE_RESPONSE_EVENT:
			findNodeResponseHandler.handle(receiveBuffer, source);
			break;
		case CommunicatorConfig.NULL_MESSAGE:
			addEitFrom(receiveBuffer[1], source);
			break;
		case CommunicatorConfig.QUEUE_EMPTY:
			Communicator.setIncQueuesEmpty();
			break;
		case CommunicatorConfig.REVOKE_QUEUE_EMPTY:
			Communicator.setDecQueuesEmpty();
			break;
		case CommunicatorConfig.STATISTICS:
			Logger.log("received statistics from " + source);
			statisticsHandler.handle(receiveBuffer, source);
			break;
		case CommunicatorConfig.FIND_NODE_TIMEOUT_EVENT:
			findNodeTimeoutHandler.handle(receiveBuffer, source);
			break;
		case CommunicatorConfig.PING_REQUEST_EVENT: {
			pingRequestEvent(receiveBuffer, source);
			break;
		}
		case CommunicatorConfig.PING_RESPONSE_EVENT: {
			pingResponseEvent(receiveBuffer, source);
			break;
		}
		case CommunicatorConfig.PING_TIMEOUT_EVENT: {
			pingTimeoutEvent(receiveBuffer, source);
			break;
		}	
		default:
			if (Logger.logEnabled)
				Logger.log("received message of unknown type " + receiveBuffer[0]);
			break;
		}
	}
	
	private static void addEitFrom(int eit, int from) {
		synchronized(Statistics.class) {
			Communicator.setEitFromMax(from, eit);
			eit = recalculateSafeUntil();
			Communicator.setEit(eit);
		}
	}
	
	private static int recalculateSafeUntil() {
		int eit = Integer.MAX_VALUE;
		int[] eitFrom = Communicator.getEitFrom();

		for (int i = 0; i < Communicator.getSize(); i++) {
			if (i != Communicator.getMyRank())
				eit = Math.min(eit, eitFrom[i]);
		}
		return eit;
	}
	
	private static void pingRequestEvent(int[] receiveBuffer, int source) {
		IPeer srcPeer = Communicator.getRemoteRankPeer(new RemoteRankPeer(receiveBuffer[2], source));

		Node destNode = Network.getById(receiveBuffer[3]);
		
		PingEvent evt = new PingEvent(PingEvent.EVT_PING, srcPeer, receiveBuffer[4], receiveBuffer[5]);
		
		Communicator.addEventFromRemoteRank(receiveBuffer[1], evt, destNode, KademliaProtocol.kademliaid, source);
	}
	
	private static void pingResponseEvent(int[] receiveBuffer, int source) {
		Node n = Network.getById(receiveBuffer[3]);
		
		if(n == null)
			return;

		IPeer rPeer = Communicator.getRemoteRankPeer(new RemoteRankPeer(receiveBuffer[2], source));

		PingEvent evt = new PingEvent(PingEvent.EVT_PING_RSP, rPeer, receiveBuffer[4], receiveBuffer[5]);
		
		evt.source = Communicator.getRemoteRankPeer(new RemoteRankPeer(receiveBuffer[2], source));

		Communicator.addEventFromRemoteRank(receiveBuffer[1], evt, n, KademliaProtocol.kademliaid, source);
	}

	private static void pingTimeoutEvent(int[] receiveBuffer, int source) {
		IPeer rPeer = Communicator.getRemoteRankPeer(new RemoteRankPeer(receiveBuffer[2], source));
		
		PingEvent evt = new PingEvent(PingEvent.EVT_PING_TMO, rPeer, receiveBuffer[4], receiveBuffer[5]);
		
		evt.source = Communicator.getRemoteRankPeer(new RemoteRankPeer(receiveBuffer[2], source));
		Node n = Network.getById(receiveBuffer[3]);
		
		Communicator.addEventFromRemoteRank(receiveBuffer[1], evt, n, KademliaProtocol.kademliaid, source);
	}
	
	private static void requestRandomOnlinePeerMsg(int source) {
		randomOnlinePeersMessage[0] = CommunicatorConfig.RESPONSE_RANDOM_ONLINE_PEER_MESSAGE_ID;

		for (int i = 0; i < CommunicatorConfig.RANDOM_ONLINE_PEERS_PER_MESSAGE; i++) {
			int id = Util.chooseRandomOnlinePeer().getId();
			randomOnlinePeersMessage[1 + i] = id;
		}
		Communicator.send(source, randomOnlinePeersMessage);
	}

	private static void responseRandomOnlinePeerMsg(int[] receiveBuffer, int source) {
		for (int i = 0; i < CommunicatorConfig.RANDOM_ONLINE_PEERS_PER_MESSAGE; i++) {
			RemoteRankPeer newPeer = new RemoteRankPeer(receiveBuffer[1 + i],
					source);
			
			int rand = CommonState.r.nextInt(CommunicatorConfig.MAX_RECEIVED_PEERS);
			
			if (Communicator.getRecvdPeer(source, rand) == null)
				Communicator.setIncNoRecvdPeers(source);
			Communicator.setRecvdPeersArray(source, rand, newPeer);
		}
	}

	private static void signalBootstrappingComplete(int source) {
		Bootstrapper.incNumRanksDone();
		Logger.log("got bootstrapping done from " + source + ", total ranks done: " + Bootstrapper.getNumRanksDone());
	}
}