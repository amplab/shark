/**
 * @author lrhodes@yahoo-inc.com
 * Created Dec 21, 2012
 */
package shark.util;

import java.io.Serializable;

/**
 * <p>The MurmurHash3_x64_128(...) is a fast, non-cryptographic, 128-bit hash 
 * function that has excellent avalanche and 2-way bit independence properties. 
 * </p>
 * 
 * <p>The C++ version, revision 147, of the MurmurHash3, written Austin Appleby,
 * and which is in the Public Domain, was the inspiration for this 
 * implementation in Java.  The C++ version can be found at 
 * <a href="http://code.google.com/p/smhasher">SMHasher & MurmurHash</a>.</p>
 * 
 * <p>This java implementation pays close attention to the C++ mathematical 
 * algorithms in order to maintain bit-wise compatibility, but the design is 
 * quite different.  This implementation has also been extended to include 
 * processing of arrays of longs, which was not part of the original C++ 
 * implementation.  This implementation 
 * will produce the same exact output hash bits as the above C++ method given 
 * the same input. Byte arrays that are a multiple of 8 bytes in length will
 * produce the same output hash as the equivalent long array in little-endian
 * byte order.</p>
 * 
 * <p>The structure of this implementation also reflects a separation of 
 * code that is dependent on the input structure (in this case byte[] or long[]) 
 * from code that is independent of the input structure. This also makes the
 * code more readable and suitable for future extensions.</p>
 * 
 * @author Lee Rhodes (lrhodes at yahoo-inc dot com)</p>
 */
public class MurmurHash3 implements Serializable {
	private static final long serialVersionUID = 0L;

	//--Hash of long[]----------------------------------------------------
	/**
	 * Returns a long array of size 2, which is a 128-bit hash of the input.
	 * @param key The input long[] array.
	 * @param seed  A long-valued seed.
	 */
	public static final long[] hash(long[] key, long seed) {
		HashState hashState = new HashState(seed, seed);
		final int length = key.length; //in longs
		
		// Number of 128-bit blocks of 2 longs, possible remainder of 1 long.
		final int nblocks = length >> 1;

		// Process the 128-bit blocks (the body) into the hash
		for (int i = 0; i < nblocks; i++) {
			long k1 = key[2*i];   //0, 2, 4, ...
			long k2 = key[2*i+1]; //1, 3, 5, ...
			hashState.blockMix128(k1, k2);
		}

		// Get the tail index, remainder length
		int tail = nblocks*2;
		int rem = length - tail;
		
		// Get the tail
		long k1 = (rem == 0)? 0: key[tail]; //k2 -> 0
		
		// Mix the tail into the hash and return
		return hashState.finalMix128(k1, 0, length*8);
	}
	
	public static final long[] hash(byte[] key, long seed) {
		return hash(key, seed, key.length);
	}

	//--Hash of byte[]----------------------------------------------------
	/**
	 * Returns a long array of size 2, which is a 128-bit hash of the input.
	 * @param key The input byte[] array.
	 * @param seed  A long-valued seed.
	 */
	public static final long[] hash(byte[] key, long seed, int length) {
		HashState hashState = new HashState(seed, seed);
		
		// Number of 128-bit blocks of 16 bytes, possible remainder of 15 bytes.
		final int nblocks = length >> 4;

		// Process the 128-bit blocks (the body) into the hash
		for (int i = 0; i < nblocks; i++) {
			long k1 = getLong(key, 16*i, 8);
			long k2 = getLong(key, 16*i+8, 8);
			hashState.blockMix128(k1, k2);
		}

		// Get the tail index, remainder length
		int tail = nblocks*16;
		int rem = length - tail;
		
		// Get the tail
		long k1 = 0;            
		long k2 = 0;
		if (rem > 8) { //k1 -> whole; k2 -> partial
			k1 = getLong(key, tail, 8);
			k2 = getLong(key, tail+8, rem-8);
		} else {       //k1 -> whole, partial or 0; k2 == 0
			k1 = (rem == 0)? 0: getLong(key, tail, rem);
		}
		
		// Mix the tail into the hash and return
		return hashState.finalMix128(k1, k2, length);
	}

	//--HashState class---------------------------------------------------
	/**
	 * Common processing of the 128-bit hash state independent of input type.
	 */
	private static class HashState {
		private static final long C1 = 0x87c37b91114253d5L;
		private static final long C2 = 0x4cf5ad432745937fL;
		private long h1;
		private long h2;
		
		HashState(long h1, long h2) {
			this.h1 = h1;
			this.h2 = h2;
		}
		
		/**
		 * Block mix (128-bit block) of input key to internal hash state.
		 * @param k1
		 * @param k2
		 */
		void blockMix128(long k1, long k2) {
			h1 ^= (k1 == 0)? 0: mixK1(k1);
			h1 = Long.rotateLeft(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			h2 ^= (k2 == 0)? 0: mixK2(k2);
			h2 = Long.rotateLeft(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;
		}
		
		long[] finalMix128(long k1, long k2, long inputLengthBytes) {
			h1 ^= (k1 == 0)? 0: mixK1(k1);  h2 ^= (k2 == 0)? 0: mixK2(k2);
			h1 ^= inputLengthBytes;         h2 ^= inputLengthBytes;
			h1 += h2;                       h2 += h1;
			h1 = finalMix64(h1);            h2 = finalMix64(h2);
			h1 += h2;                       h2 += h1;
			return (new long[] { h1, h2 });
		}
		
		/**
		 * Final self mix of h*.
		 * @param h
		 */
		private static final long finalMix64(long h) { 
			h ^= h >>> 33;
			h *= 0xff51afd7ed558ccdL;
			h ^= h >>> 33;
			h *= 0xc4ceb9fe1a85ec53L;
			h ^= h >>> 33;
			return h;
		}
		
		/**
		 * Self mix of k1
		 * @param k1
		 */
		private static final long mixK1(long k1) {
			k1 *= C1;
			k1 = Long.rotateLeft(k1, 31);
			k1 *= C2;
			return k1;
		}
		
		/**
		 * Self mix of k2
		 * @param k2
		 */
		private static final long mixK2(long k2) {
			k2 *= C2;
			k2 = Long.rotateLeft(k2, 33);
			k2 *= C1;
			return k2;
		}
	}
	
	//--Helper methods----------------------------------------------------
	/**
	 * Gets a long from the given byte array starting at the given byte array 
	 * index and continuing for remainder (rem) bytes. 
	 * There is no limit checking.
	 * @param bArr The given input byte array.
	 * @param index Byte index from the start of the array.
	 * @param rem Remainder bytes. An integer in the range [1,8].
	 */
	private static final long getLong(byte[] bArr, int index, int rem) {
		long out = 0L;
		for (int i=rem; i-- > 0; ) {
			byte b = bArr[index + i];
			out ^= (b & 0xFFL) << i*8; //equivalent to |=
		}
		return out;
	}
}