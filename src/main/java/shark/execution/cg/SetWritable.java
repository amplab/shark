package shark.execution.cg;

import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class SetWritable {
	public static final BooleanWritable build(BooleanWritable w, boolean v) {
		w.set(v);
		return w;
	}
	
	public static final ByteWritable build(ByteWritable w, byte v) {
		w.set(v);
		return w;
	}
	
	public static final BytesWritable build(BytesWritable w, byte[] v) {
		w.set(v, 0, v.length);
		return w;
	}
	
	public static final Text build(Text w, String v) {
		w.set(v);
		return w;
	}
	
	public static final ShortWritable build(ShortWritable w, short v) {
		w.set(v);
		return w;
	}
	
	public static final FloatWritable build(FloatWritable w, float v) {
		w.set(v);
		return w;
	}
	
	public static final IntWritable build(IntWritable w, int v) {
		w.set(v);
		return w;
	}
	
	public static final LongWritable build(LongWritable w, long v) {
		w.set(v);
		return w;
	}
	
	public static final DoubleWritable build(DoubleWritable w, double v) {
		w.set(v);
		return w;
	}
	public static final TimestampWritable build(TimestampWritable w, Timestamp v) {
		w.set(v);
		return w;
	}
}

