package peersim.MPI;

public class CommUtil {

	public static void intToByteArray(byte[] ref, int offset, int i) {
		ref[offset] = (byte) ((i << 24) >>> 24);
		ref[offset + 1] = (byte) ((i << 16) >>> 24);
		ref[offset + 2] = (byte) ((i << 8) >>> 24);
		ref[offset + 3] = (byte) (i >>> 24);
	}
}