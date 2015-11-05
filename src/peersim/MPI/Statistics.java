package peersim.MPI;

import java.util.Timer;
import java.util.TimerTask;

import peersim.MPI.MessageHandler.IMessageHandler;
import peersim.core.CommonState;
import peersim.edsim.EDSimulator.ExecuteResult;

public class Statistics implements IMessageHandler {

	static class EitSampler extends TimerTask {
		@Override
		public void run() {

			synchronized (Statistics.class) {
				int time = CommonState.getIntTime();
				
				if(time == 0)
					return;

				if (Statistics.samplingRefTime != -1)
					return;
				Statistics.addEitDistance(Communicator.getEit() - time);

				Statistics.setSamplingReferenceTime(time);

				for (int i = 0; i < Communicator.getSize(); i++) {
					if (i == Communicator.getMyRank())
						continue;
					int eot = Communicator.getEot(i);
					Statistics.addEitDistanceFrom(eot - time, i);
				}
			}
		}
	}

	public static final int FINE_CLOCK_SIMULATION = 0;
	public static final int FINE_CLOCK_BOOTSTRAPPING = 1;
	public static final int FINE_CLOCK_EXECUTE_NEXT = 2;
	public static final int FINE_CLOCK_NULL_MESSAGE = 3;
	public static final int FINE_CLOCK_FORWARD_EVENT = 4;
	public static final int FINE_CLOCK_HANDLE_MESSAGE = 5;

	private static final int NUM_FINE_CLOCKS = 6;

	private static final String[] fineClockName = { "simulation",
			"bootstrapping", "executeNext", "sendNullMessage", "forwardEvent",
			"handle message" };

	public static final boolean fineClockEnabled = true;
	public static final boolean coarseClockEnabled = false;
	public static final boolean countEvents = true;
	public static final boolean averageEit = false;

	private static long nullMessagesSent = 0;

	private static long[] fineClockStartNs = new long[fineClockName.length];
	private static long[] fineClockSumNs = new long[fineClockName.length];

	private static long durations[] = new long[ExecuteResult.values().length];
	private static long startNs = 0;
	private static ExecuteResult previousExecResult = ExecuteResult.DUMMY;

	public static final int EVENT_COUNTER_LOCAL_MESSAGE = 0;
	public static final int EVENT_COUNTER_REMOTE_MESSAGE = 1;

	private static final String[] eventCounterName = { "localMessage",
			"remoteMessage" };
	private static long[] eventCounter = new long[eventCounterName.length];

	private static long eitSum = 0;
	private static long eitCount = 0;

	private static int executedEventsInCurrentPeriod = 0;

	private static double executePeriodTimeSum = 0;
	private static double executePeriodNumEventsSum = 0;

	private static double waitPeriodTimeSum = 0;

	private static int executePeriodObservations = 0;
	private static int waitPeriodObservations = 0;

	private static int onlinePeers = 0, totalPeers = 0, msg_deliv = 0;
	private static float lookupCostMin = Integer.MAX_VALUE,
			lookupCostAvgSum = 0, lookupCostMax = 0;
	private static float timeMin = Integer.MAX_VALUE, timeAvgSum = 0,
			timeMax = 0;
	private static long timeoutFindNode = 0, findNodeQueries = 0,
			peersAdded = 0, peersRemoved = 0;
	private static int pcostSum = 0;

	private static int statsReceived = 0;

	private static float eotQualitySum = 0;
	private static int eotQualityObservations = 0;
	private static float eitQualitySum = 0;
	private static int eitQualityObservations = 0;

	private static volatile int refEitFrom[];

	private static int samplingRefTime = -1;

	private static int eitSamplingPeriodMs = 1000;

	private static volatile int refEit;

	private static int eitFromSum = 0;

	private static int eitFromCount = 0;

	public static boolean observeEit = true;

	private static Timer observeEitTimer;
	private static volatile long minIncomingTimestampGlobal = -1;
	private static volatile long[] minIncomingTimestamp;

	public static void init() {

		minIncomingTimestamp = new long[Communicator.getSize()];

		if (observeEit) {
			observeEitTimer = new Timer();

			observeEitTimer.scheduleAtFixedRate(new EitSampler(),
					eitSamplingPeriodMs, eitSamplingPeriodMs);

			refEitFrom = new int[Communicator.getSize()];
			for (int i = 0; i < Communicator.getSize(); i++) {
				refEitFrom[i] = -1;
				minIncomingTimestamp[i] = -1;
			}
		}
	}

	public static void setSamplingReferenceTime(int intTime) {
		samplingRefTime = intTime;
	}

	public static void addEitDistanceFrom(int eit, int rank) {
		eitFromSum += eit;
		eitFromCount++;
		refEitFrom[rank] = eit;
	}

	public static void coarseClock(ExecuteResult execResult) {
		if (execResult == previousExecResult)
			return;

		long nowNs = System.nanoTime();

		if (previousExecResult != ExecuteResult.DUMMY)
			durations[previousExecResult.ordinal()] += nowNs - startNs;

		previousExecResult = execResult;
		startNs = nowNs;
	}

	public static void fineClockStart(int clockNum) {
		fineClockStartNs[clockNum] = System.nanoTime();
	}

	public static void fineClockFinish(int clockNum) {
		long diffNs = System.nanoTime() - fineClockStartNs[clockNum];
		fineClockSumNs[clockNum] += diffNs;

	}

	public static void printEventCounts() {
		for (int i = 0; i < eventCounterName.length; i++)
			Logger.log("events of type " + eventCounterName[i] + ": "
					+ eventCounter[i]);
	}

	public static boolean printStats() {
		if(Statistics.observeEit)
			observeEitTimer.cancel();

		if (statsReceived < Communicator.getSize())
			return false;

		// wait for other ranks to be quiet
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}

		Logger.log("sent " + nullMessagesSent + " null messages.");

		if (coarseClockEnabled) {
			long sumDuration = 0;
			for (ExecuteResult execResult : ExecuteResult.values())
				sumDuration += durations[execResult.ordinal()];

			sumDuration /= 1000000;

			Logger.log("coarse durations:");
			Logger.log("total: " + sumDuration + " ms");
			for (ExecuteResult execResult : ExecuteResult.values()) {
				long ms = durations[execResult.ordinal()] / 1000000;
				Logger.log(execResult.name() + ": " + ms + " ms ("
						+ String.format("%.2f", 100.0 * ms / sumDuration)
						+ "%)");
			}
		}

		if (fineClockEnabled)
			Logger.log("\nfine durations (non-orthogonal!):");

		for (int i = 0; i < NUM_FINE_CLOCKS; i++) {
			Logger.log(fineClockName[i] + ": " + fineClockSumNs[i] / 1000000
					+ " ms");
		}

		printEventCounts();

		Logger.log("\n");
		Runtime runtime = Runtime.getRuntime();
		for (int i = 0; i < 10; i++)
			runtime.gc();
		long memoryUsage = (runtime.totalMemory() - runtime.freeMemory())
				/ (1024 * 1024);
		Logger.log("final memory usage: " + memoryUsage + " MB");
		
		if (eitCount > 0) {
			float eitAvg = (float) eitSum / eitCount;
			Logger.log("average eit distance: " + eitAvg);
		}
		if (eitFromCount > 0) {
			float eitFromAvg = (float) eitFromSum / eitFromCount;
			Logger.log("average eot distance: " + eitFromAvg);

		}

		Logger.log("average eot quality: " + 100 * eotQualitySum
				/ eotQualityObservations + " %");

		Logger.log("average eit quality: " + 100 * eitQualitySum
				/ eitQualityObservations + " %");


		printPeriods();

		if (Communicator.getMyRank() == 0) {
			System.out.println("#Overall:");
			System.out
					.println(String
							.format("#\tpeers\t\tlookups\tmsgs/lookup\t\t\tlookup time [ms]"));
			System.out.println(String.format("#Time(s)online\ttotal\t\t"
					+ "min\tavg\tmax\t" + "+pings\t" + "min\tavg\tmax"));

			int size = Communicator.getSize();

			String s = String.format("%d\t%d\t%d\t%d\t"
					+ "%3.1f\t%3.1f\t%3.1f\t" + "%3.1f\t" + "%d\t%d\t%d\n",
					CommonState.getTime() / 1000, onlinePeers, totalPeers,
					msg_deliv, lookupCostMin, lookupCostAvgSum / size,
					lookupCostMax, (lookupCostAvgSum + pcostSum) / size,
					(int) timeMin, (int) (timeAvgSum / size), (int) timeMax);

			s += String.format(
					"#%d of %d (= %2.1f%%) requests caused timeouts",
					timeoutFindNode, findNodeQueries,
					((float) timeoutFindNode / findNodeQueries) * 100.0);
			System.out.println(s);
			System.out.println(String.format(
					"#%d peers added, %d peers removed during simulation.",
					peersAdded, peersRemoved));
			return true;
		}

		return false;

	}

	public static void incNullMessagesSent() {
		nullMessagesSent += Communicator.getSize() - 1;
	}

	public static void incNoEvents(int type) {
		eventCounter[type]++;
	}

	public static void addEitDistance(int eit) {
		refEit = eit;
		eitSum += eit;
		eitCount++;
	}

	@Override
	public void handle(int[] message, int source) {
		Logger.log("got stats from rank " + source);

		statsReceived++;
		Statistics.onlinePeers += message[1];
		Statistics.totalPeers += message[2];
		Statistics.msg_deliv += message[3];
		Statistics.lookupCostMin = Math.min(Statistics.lookupCostMin,
				message[4]);
		Statistics.lookupCostAvgSum += message[5];
		Statistics.lookupCostMax = Math.max(Statistics.lookupCostMax,
				message[6]);

		Statistics.pcostSum += message[7];

		Statistics.timeMin = Math.min(Statistics.timeMin, message[8]);
		Statistics.timeAvgSum += message[9];
		Statistics.timeMax = Math.max(Statistics.timeMax, message[10]);

		Statistics.timeoutFindNode += message[11];
		Statistics.findNodeQueries += message[12];
		Statistics.peersAdded += message[13];
		Statistics.peersRemoved += message[14];

		Logger.log("rank " + source + " performed " + message[12] + " queries");
	}

	@Override
	public void handle(float[] message, int source) {
		Logger.log("got stats from rank " + source);

		statsReceived++;
		Statistics.onlinePeers += message[1];
		Statistics.totalPeers += message[2];
		Statistics.msg_deliv += message[3];
		Statistics.lookupCostMin = Math.min(Statistics.lookupCostMin,
				message[4]);
		Statistics.lookupCostAvgSum += message[5];
		Statistics.lookupCostMax = Math.max(Statistics.lookupCostMax,
				message[6]);

		Statistics.pcostSum += message[7];

		Statistics.timeMin = Math.min(Statistics.timeMin, message[8]);
		Statistics.timeAvgSum += message[9];
		Statistics.timeMax = Math.max(Statistics.timeMax, message[10]);

		Statistics.timeoutFindNode += message[11];
		Statistics.findNodeQueries += message[12];
		Statistics.peersAdded += message[13];
		Statistics.peersRemoved += message[14];

		Logger.log("rank " + source + " performed " + message[12] + " queries");
	}

	public static void transmitStats(int onlinePeers, int totalPeers,
			float msg_deliv, float lookupCostMin, float lookupCostAvg,
			float lookupCostMax, int pcost, float timeMin, float timeAvg,
			float timeMax, long timeoutFindNode, long findNodeQueries,
			int peersAdded, int peersRemoved) {

		if (!Communicator.areAllQueuesEmpty())
			return;

		Logger.log("transmitting stats");
		float[] message = new float[15];
		message[0] = CommunicatorConfig.STATISTICS;

		message[1] = onlinePeers;
		message[2] = totalPeers;
		message[3] = msg_deliv;
		message[4] = lookupCostMin; // float
		message[5] = lookupCostAvg; // float
		message[6] = lookupCostMax; // float

		message[7] = pcost;

		message[8] = timeMin;
		message[9] = timeAvg;
		message[10] = timeMax;

		message[11] = timeoutFindNode;
		message[12] = findNodeQueries;
		message[13] = peersAdded;
		message[14] = peersRemoved;

		if (Communicator.getSize() > 1)
			Communicator.send(0, message);
		else
			MessageParser.parseMessage(message, 0);
	}

	public static void transmitStats(int onlinePeers, int totalPeers,
			int msg_deliv, int lookupCostMin, int lookupCostAvg,
			int lookupCostMax, int pcost, int timeMin, int timeAvg,
			int timeMax, int timeoutFindNode, int findNodeQueries,
			int peersAdded, int peersRemoved) {

		if (!Communicator.areAllQueuesEmpty())
			return;

		Logger.log("transmitting stats");
		int[] message = new int[15];
		message[0] = CommunicatorConfig.STATISTICS;

		message[1] = onlinePeers;
		message[2] = totalPeers;
		message[3] = msg_deliv;
		message[4] = lookupCostMin; // float
		message[5] = lookupCostAvg; // float
		message[6] = lookupCostMax; // float

		message[7] = pcost;

		message[8] = timeMin;
		message[9] = timeAvg;
		message[10] = timeMax;

		message[11] = timeoutFindNode;
		message[12] = findNodeQueries;
		message[13] = peersAdded;
		message[14] = peersRemoved;

		if (Communicator.getSize() > 1)
			Communicator.send(0, message);
		else
			MessageParser.parseMessage(message, 0);
	}

	public static void addWaitPeriod(int ns) {
		waitPeriodTimeSum += ns;
		waitPeriodObservations++;
	}

	public static void addExecutePeriod(int ns) {
		executePeriodTimeSum += ns;
		executePeriodNumEventsSum += executedEventsInCurrentPeriod;
		executePeriodObservations++;
	}

	public static void addEotQuality(long base, long predictedDelta,
			long observed) {

		long observedDelta = observed - base;
		
		if (observedDelta != 0) {
			eotQualitySum += (float) predictedDelta / observedDelta;
		}
		eotQualityObservations++;
	}


	public static void addEitQuality(long base, long predictedDelta,
			long observed) {

		
		long observedDelta = observed - base;
		
		if (observedDelta != 0) {
			eitQualitySum += (float) predictedDelta / observedDelta;
		}
		eitQualityObservations++;
	}

	public static void printPeriods() {
		double executePeriodTimeAvg = executePeriodTimeSum
				/ executePeriodObservations;
		double executePeriodNumEventsAvg = executePeriodNumEventsSum
				/ executePeriodObservations;
		double waitPeriodTimeAvg = waitPeriodTimeSum / waitPeriodObservations;

		Logger.log("average execute period time: "
				+ (executePeriodTimeAvg / 1000) + "µs");
		Logger.log("average execute period #events: "
				+ executePeriodNumEventsAvg);
		Logger.log("average wait period time: " + (waitPeriodTimeAvg / 1000)
				+ "µs");

	}

	public static void registerRemoteEvent(int from, int absoluteTime) {
		synchronized (Statistics.class) {
			if (samplingRefTime != -1) {
				
				minIncomingTimestampGlobal = minIncomingTimestampGlobal == -1 ? absoluteTime
						: Math.min(minIncomingTimestampGlobal, absoluteTime);
				
				
				if(refEitFrom[from] != -1)
					minIncomingTimestamp[from] = minIncomingTimestamp[from] == -1 ? absoluteTime
							: Math.min(minIncomingTimestamp[from], absoluteTime);
				
				long currentTime = CommonState.getTime();

				if (minIncomingTimestampGlobal != -1
						&& currentTime >= minIncomingTimestampGlobal) {

					addEitQuality(samplingRefTime, refEit,
							minIncomingTimestampGlobal);
					minIncomingTimestampGlobal = -1;
				}

				boolean allRanksDone = true;
				for (int i = 0; i < Communicator.getSize(); i++) {
					if (refEitFrom[i] != -1) {
						if (minIncomingTimestamp[i] != -1
								&& currentTime >= minIncomingTimestamp[i]) {
							
							addEotQuality(samplingRefTime, refEitFrom[i],
									minIncomingTimestamp[i]);
							minIncomingTimestamp[i] = -1;
							refEitFrom[i] = -1;
						} else {
							allRanksDone = false;
						}

					}
				}
				if (allRanksDone) {
					samplingRefTime = -1;
					return;
				}



			}
		}
	}
}
