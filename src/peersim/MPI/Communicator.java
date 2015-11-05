package peersim.MPI;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.edsim.EDSimulator.ExecuteResult;
import peersim.kademlia.IPeer;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.events.KademliaEvent;

public class Communicator {
	private static int eit = 0;
	private static int[] eitFrom;

	private static boolean initialized;

	private static MessageReceiver messageReceiver;

	private static Thread messageReceiverThread;
	private static Lock mutex = new ReentrantLock();

	private static boolean myQueueEmpty = false;

	private static int myRank;

	private static int noReceivedPeers[];

	private static int nullMessage[] = new int[2];

	private static OutputStream os;
	private static byte[] outPipeBuffer;

	private static int previousEot = -1;
	
	private static int queuesEmpty = 0;

	private static RemoteRankPeer[][] receivedPeersArray;

	private static Map<Integer, IPeer> remoteRankPeers = new HashMap<Integer, IPeer>();
	
	private static int size;
	
	public static void acquireLock() {
		mutex.lock();
	}
	
	public static void setIncNoRecvdPeers(int source) {
		noReceivedPeers[source]++;
	}
	
	public static void setRecvdPeersArray(int source, int rand, RemoteRankPeer value) {
		receivedPeersArray[source][rand] = value;
	}
	
	public static RemoteRankPeer getRecvdPeer(int source, int rand) {
		return receivedPeersArray[source][rand];
	}

	public static void addEventFromRemoteRank(int absoluteTime, Object event,
			Node node, int pid, int from) {

		revokeQueueEmpty();
		
		long currentTime = CommonState.getTime();

		if (absoluteTime < currentTime) {
			Logger.log("BUG: got an event with timestamp " + absoluteTime
					+ ", which is lower than current time" + currentTime
					+ ", we're supposed to be safe until " + eit);
		}

		if(Statistics.observeEit)
			Statistics.registerRemoteEvent(from, absoluteTime);

		EDSimulator.add(absoluteTime - currentTime, event, node, pid);
	}

	public static boolean areAllQueuesEmpty() {
		assert (queuesEmpty <= size);

		return queuesEmpty == size;
	}

	public static void setDecQueuesEmpty() {
		queuesEmpty--;
	}

	public static void finalGracePeriod() {
		if(size == 1)
			return;
		long startMs = System.currentTimeMillis();
		long nowMs;
		do {
			nowMs = System.currentTimeMillis();
			receive(null);
		} while(nowMs - startMs < CommunicatorConfig.GRACE_PERIOD_MS && areAllQueuesEmpty());
	}

	public static int getEit() {
		return eit;
	}

	public static int[] getEitFrom() {
		return eitFrom;
	}

	public static int getEot(int i) {
		return eitFrom[i];
	}

	public static IPeer getLastReceivedRandomOnlinePeer(int rankToReceiveFrom) {
		int requestsSent = 0;
		while (true) {
			receive(null);
			if (noReceivedPeers[rankToReceiveFrom] < CommunicatorConfig.MIN_RECEIVED_PEERS && requestsSent < 10) {
				sendGetRandomOnlinePeer(rankToReceiveFrom);
				requestsSent++;
			}
			if(noReceivedPeers[rankToReceiveFrom] > 0) {
				int i;
				RemoteRankPeer tmpPeer; 
				do {
					i = CommonState.r.nextInt(CommunicatorConfig.MAX_RECEIVED_PEERS);
					tmpPeer = receivedPeersArray[rankToReceiveFrom][i];
				} while(tmpPeer == null /* || (tmpPeer.getRank() != rankToReceiveFrom && ++tries < 1000) */);
				receivedPeersArray[rankToReceiveFrom][i] = null;
				noReceivedPeers[rankToReceiveFrom]--;

				RemoteRankPeer receivedPeer = (RemoteRankPeer) getRemoteRankPeer(tmpPeer); 
					
				return receivedPeer;
			}
		}
	}

	public static int getLookahead() {
		return CommunicatorConfig.MIN_REMOTE_RTT / 2;
	}

	public static int getMyRank() {
		if (!initialized)
			throw new IllegalStateException("getMyRank() called, but Communicator is not initialized yet");
		return myRank;
	}

	public static int getQueuesEmpty() {
		return queuesEmpty;
	}
	
	public static IPeer getRemoteRankPeer(int id) {
		return remoteRankPeers.get(id);
	}

	public static IPeer getRemoteRankPeer(RemoteRankPeer tmpPeer) {
		int id = tmpPeer.getId();
		int rank = tmpPeer.getRank();
		IPeer peer = remoteRankPeers.get(id);
		if (peer == null) {
			peer = new RemoteRankPeer(id, rank);
			remoteRankPeers.put(id, peer);
		}
		return peer;
	}

	public static int getSize() {
		return size;
	}

	public static void setIncQueuesEmpty() {
		queuesEmpty++;
	}
	
	public static void init(String[] args) {
		initialized = true;
		
		CommunicatorConfig.setupCommunicator();
		outPipeBuffer = new byte[CommunicatorConfig.MAX_MESSAGE_LENGTH * 4];

		Logger.log("number of arguments: " + args.length);
		for(int i = 0; i < args.length; i++)
			Logger.log("arg " + i + ": " + args[i]);
		
		size = Integer.valueOf(args[1]);
		myRank = Integer.valueOf(args[2]);
		String pipeId = args[3];
		
		receivedPeersArray = new RemoteRankPeer[size][CommunicatorConfig.MAX_RECEIVED_PEERS];
		noReceivedPeers = new int[size];
		
		for(int i = 0; i < size; i++)
			noReceivedPeers[i] = 0;
		
		eitFrom = new int[size];

		if (Logger.logEnabled) {
			Logger.log("size: " + size);
			Logger.log("my rank: " + myRank);
			Logger.log("pipeId: " + pipeId);
		}

		acquireLock();

		if(size > 1) {
			messageReceiver = new MessageReceiver(pipeId);
			messageReceiverThread = new Thread(messageReceiver);
			messageReceiverThread.start();
		
			Path path = FileSystems.getDefault().getPath("pipes/javaToCPipe_" + pipeId);
			Logger.log("opening pipe " + path);

			try {
				os = Files.newOutputStream(path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void receive(ExecuteResult execResult) {
		if(messageReceiver.getMessageIsPending()) {
			//int messageCount = messageReceiver.receivedMessages;
			releaseLock();
			while(messageReceiver.getMessageIsPending()) {
				// do nothing
			}
			acquireLock();
			//Logger.log("got " + (messageReceiver.receivedMessages - messageCount) + " messages");
		}
	}

	public static void releaseLock() {
		mutex.unlock();
	}

	public static void scheduleEvent(long delay, IPeer forPeer, KademliaEvent evt) {
		if(forPeer instanceof RemoteRankPeer) {
			if(Statistics.countEvents)
				Statistics.incNoEvents(Statistics.EVENT_COUNTER_REMOTE_MESSAGE);
			
			RemoteRankPeer rPeer = (RemoteRankPeer)forPeer;
			
			if(Statistics.fineClockEnabled)
				Statistics.fineClockStart(Statistics.FINE_CLOCK_FORWARD_EVENT);
			EventForwarder.forwardEvent(delay, evt, rPeer);
			
			if(Statistics.fineClockEnabled)
				Statistics.fineClockFinish(Statistics.FINE_CLOCK_FORWARD_EVENT);
		} else {
			if(Statistics.countEvents)
				Statistics.incNoEvents(Statistics.EVENT_COUNTER_LOCAL_MESSAGE);
			
			if(forPeer.toNode() == null) {
				Logger.log("BUG: scheduling event for null node, id: " + forPeer.getId());
				System.exit(1);
			}
			
			EDSimulator.add(delay, evt, forPeer.toNode(), KademliaProtocol.kademliaid);
		}
	}

	// send a message using floats
	public static void send(int targetRank, float message[]) {
		int msgLength = message.length + 2;
		
		CommUtil.intToByteArray(outPipeBuffer, 0, msgLength);
		CommUtil.intToByteArray(outPipeBuffer, 4, targetRank);
		CommUtil.intToByteArray(outPipeBuffer, 8, CommunicatorConfig.CONTENT_TYPE_FLOATS);

		for(int i = 0; i < message.length; i++) {
			int bytePos = (i + 3) * 4;
			int m = Float.floatToIntBits(message[i]);
			CommUtil.intToByteArray(outPipeBuffer, bytePos, m);
		}
		
		try {
			os.write(outPipeBuffer, 0, (message.length + 3) * 4);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// send a message using ints
	public static void send(int targetRank, int message[]) {
		int msgLength = message.length + 2;
		
		CommUtil.intToByteArray(outPipeBuffer, 0, msgLength);
		CommUtil.intToByteArray(outPipeBuffer, 4, targetRank);
		CommUtil.intToByteArray(outPipeBuffer, 8, CommunicatorConfig.CONTENT_TYPE_INTS);
		
		for(int i = 0; i < message.length; i++) {
			int bytePos = (i + 3) * 4;
			int m = message[i];
			CommUtil.intToByteArray(outPipeBuffer, bytePos, m);
		}
		
		try {
			os.write(outPipeBuffer, 0, (message.length + 3) * 4);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void sendNullMessage(int eot) {
		if (eot == previousEot) {
			return;
		}
		
		if (Statistics.fineClockEnabled)
			Statistics.fineClockStart(Statistics.FINE_CLOCK_NULL_MESSAGE);

		Statistics.incNullMessagesSent();

		previousEot = eot;
		// previousEotTime = CommonState.getIntTime();

		nullMessage[0] = CommunicatorConfig.NULL_MESSAGE;
		nullMessage[1] = eot;

		broadcast(nullMessage);

		if (Statistics.fineClockEnabled)
			Statistics.fineClockFinish(Statistics.FINE_CLOCK_NULL_MESSAGE);
	}

	public static void setEit(int newEit) {
		eit = newEit;
	}

	public static void setEitFromMax(int from, int eit) {
		eitFrom[from] = Math.max(eitFrom[from], eit);
	}
	
	public static void shutdown() {
		 Statistics.fineClockFinish(Statistics.FINE_CLOCK_SIMULATION);
		 
		 while(myRank == 0 && !Statistics.printStats())
			 receive(null);
		 
		 Statistics.printEventCounts();
	 
		 if(size > 1) {
			 messageReceiverThread.interrupt();
			 
			 
			 byte[] mpiWrapperShutdownMessage = new byte[4];
			 int shutdownSignal = -1;
			 
			 CommUtil.intToByteArray(mpiWrapperShutdownMessage, 0, shutdownSignal);
			 
			 try {
				os.write(mpiWrapperShutdownMessage, 0, 4);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void signalQueueEmpty() {
		if (myQueueEmpty)
			return;

		int message[] = new int[1];

		message[0] = CommunicatorConfig.QUEUE_EMPTY;
		queuesEmpty++; // own queue is empty, apparently
		myQueueEmpty = true;

		broadcast(message);
	}
	
	private static void broadcast(int message[]) {
		for (int i = 0; i < size; i++)
			if (i != myRank)
				send(i, message);
	}
	
	private static void revokeQueueEmpty() {
		if (!myQueueEmpty)
			return;

		int message[] = new int[1];
		message[0] = CommunicatorConfig.REVOKE_QUEUE_EMPTY;
		queuesEmpty--;
		broadcast(message);

		myQueueEmpty = false;
	}

	private static void sendGetRandomOnlinePeer(int targetRank) {
		int[] message = new int[1];
		message[0] = CommunicatorConfig.REQUEST_RANDOM_ONLINE_PEER_MESSAGE_ID;

		send(targetRank, message);
	}
}