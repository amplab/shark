package shark.execution.cg;

import org.apache.hadoop.io.BytesWritable;

public class SetRaw {
	public static final byte[] getBytes(BytesWritable bw) {
		byte[] a = new byte[bw.getLength()];
		System.arraycopy(bw.getBytes(), 0, a, 0, a.length);
		
		return a;
	}
}

