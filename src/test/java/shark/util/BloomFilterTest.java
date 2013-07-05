package shark.util;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.primitives.Ints;

public class BloomFilterTest {

	@Test
	public void testString() {
		BloomFilter<String> bf = new BloomFilter<String>(0.03, 1000000);
		for (int i=0;i< 1000000; i++) {
			bf.add(i + "0");
		}
		assertTrue(bf.contains(9000 + "0"));
		assertFalse(bf.contains(9000 + "1"));
	}

	@Test
	public void testByteArray() {
		BloomFilter<Byte[]> bf = new BloomFilter<Byte[]>(0.03, 1000000);
		for (int i=0;i< 1000000; i+= 10) {
			bf.add(Ints.toByteArray(i));
		}
		assertTrue(bf.contains(Ints.toByteArray(9000)));
		assertFalse(bf.contains(Ints.toByteArray(9001)));
	}
	
	@Test
	public void testText() {
		BloomFilter<String> bf = new BloomFilter<String>(0.03, 1000000);
		for (int i=0;i< 1000000; i+= 10) {
			Text txt = new Text(i + "0");
			bf.add(txt.toString());
		}
		
	}
}
