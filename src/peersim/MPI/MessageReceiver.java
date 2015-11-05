package peersim.MPI;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;


public class MessageReceiver implements Runnable {

	private volatile boolean messageIsPending = false;
	
	private static InputStream is; // should be replaced by DataInputStream so conversions from to byte[] do not need to be done manually
	private static byte[] inPipeBuffer = new byte[CommunicatorConfig.MAX_MESSAGE_LENGTH * 4];
	private static int[] inPipeBufferInts = new int[CommunicatorConfig.MAX_MESSAGE_LENGTH];
	private static float[] inPipeBufferFloats = new float[CommunicatorConfig.MAX_MESSAGE_LENGTH];
	
	public MessageReceiver(String pipeId) {
		Path path = FileSystems.getDefault().getPath("pipes/cToJavaPipe_" + pipeId);
		Logger.log("opening pipe " + path);

		try {
			is = Files.newInputStream(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		int length;
		
		try {
			while(true) {
				is.read(inPipeBuffer, 0, 4);
				messageIsPending = true;

				length = inPipeBuffer[0] & 0xFF | (inPipeBuffer[1] & 0xFF) << 8 | (inPipeBuffer[2] & 0xFF) << 16 | (inPipeBuffer[3] & 0xFF) << 24; // little endian!
				
				is.read(inPipeBuffer, 0, 4);
			
				int source = inPipeBuffer[0] & 0xFF | (inPipeBuffer[1] & 0xFF) << 8 | (inPipeBuffer[2] & 0xFF) << 16 | (inPipeBuffer[3] & 0xFF) << 24; // little endian!
				
				is.read(inPipeBuffer, 0, 4);
				int contentType = inPipeBuffer[0] & 0xFF | (inPipeBuffer[1] & 0xFF) << 8 | (inPipeBuffer[2] & 0xFF) << 16 | (inPipeBuffer[3] & 0xFF) << 24; // little endian!
				
				if(contentType == CommunicatorConfig.CONTENT_TYPE_INTS) {
					//Logger.log("content type is int");
				} else {
					Logger.log("content type of message from " + source + " is float");
				}
					
				length--;
				
				is.read(inPipeBuffer, 0, length * 4);
				
				for(int i = 0; i < length; i++) {
					int bytePos = i * 4;
					int currInt = inPipeBuffer[bytePos] & 0xFF | (inPipeBuffer[bytePos + 1] & 0xFF) << 8 | (inPipeBuffer[bytePos + 2] & 0xFF) << 16 | (inPipeBuffer[bytePos + 3] & 0xFF) << 24; // little endian!
					if(contentType == CommunicatorConfig.CONTENT_TYPE_INTS)
						inPipeBufferInts[i] = currInt;
					else if(contentType == CommunicatorConfig.CONTENT_TYPE_FLOATS) {
						inPipeBufferFloats[i] = Float.intBitsToFloat(currInt);
					}
						
				}

				Communicator.acquireLock();
				
				if(Statistics.fineClockEnabled)
					Statistics.fineClockStart(Statistics.FINE_CLOCK_HANDLE_MESSAGE);
				
				messageIsPending = false;
				
				if(contentType == CommunicatorConfig.CONTENT_TYPE_INTS)
					MessageParser.parseMessage(inPipeBufferInts, source);
				else if(contentType == CommunicatorConfig.CONTENT_TYPE_FLOATS)
					MessageParser.parseMessage(inPipeBufferFloats, source);				
				
				Communicator.releaseLock();
				if(Statistics.fineClockEnabled)
					Statistics.fineClockFinish(Statistics.FINE_CLOCK_HANDLE_MESSAGE);
			}

		} catch (IOException e) {
			if(e instanceof ClosedByInterruptException)
				Logger.log("messageReceiver is done");
			else
				e.printStackTrace();
		}
	}

	public boolean getMessageIsPending() {
		return messageIsPending;
	}
}
