package org.privman.lior.bytebufferbitset;

import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE_DIRECT;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Similar to {@link java.util.BitSet BitSet}, but backed by a
 * {@link java.nio.ByteBuffer ByteBuffer}. Differences with {@code BitSet}
 * include:
 * <p>
 * {@code BufferBitSet}
 * <ul>
 * <li>... is neither {@code Cloneable} nor {@code Serializable}.
 * <li>... does not hide the backing buffer, and offers copy-free constructors
 * for wrapping an existing buffer.
 * <li>... allows for specifying the {@link ResizeBehavior resize} behavior.
 * </ul>
 * This bitset is not thread safe, and concurrent writes could put it into a bad
 * state. External modifications to the backing buffer can do the same.
 * 
 * @author Lior Privman
 * @author Arthur van Hoff (java.util.BitSet)
 * @author Michael McCloskey (java.util.BitSet)
 * @author Martin Buchholz (java.util.BitSet)
 * 
 * @see java.util.BitSet
 * @see java.nio.ByteBuffer
 */
public class BufferBitSet {

	private static final int INITIAL_SIZE = 8;

	private static final int MASK = 0xFF;

	/** {@link ResizeBehavior} */
	private final ResizeBehavior resizeBehavior;

	/**
	 * This buffer's limit should always be equal to its capacity.
	 * <p>
	 * The position is used to track how many bytes are actually in use.
	 */
	private ByteBuffer buffer;

	/*--------------------------------------------------------------------------------
	 *  Getters
	 *-------------------------------------------------------------------------------*/
	/**
	 * Return the {@link ByteBuffer} backing this {@link BufferBitSet}.
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}

	/**
	 * Return this {@link BufferBitSet bitset's} {@link ResizeBehavior resize}
	 * behavior.
	 */
	public ResizeBehavior getResizeBehavior() {
		return resizeBehavior;
	}

	/*--------------------------------------------------------------------------------
	 *  Constructors and factory methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Internal constructor, allows for specifying whether or not the buffer was
	 * supplied externally.
	 */
	private BufferBitSet(ByteBuffer buffer, ResizeBehavior resizeBehavior, boolean external) {

		this.resizeBehavior = resizeBehavior;

		if (external) {
			this.buffer = buffer.slice();

			// TODO: zero-out as we go (tracking highwater mark)
			byte zero = (byte) 0;
			for (int i = 0; i < buffer.limit(); i++)
				this.buffer.put(i, zero);
		} else {
			this.buffer = buffer;
		}
	}

	/**
	 * Creates a {@link BufferBitSet} with {@code ResizeBehavior}
	 * {@link ResizeBehavior#ALLOCATE ALLOCATE}.
	 */
	public BufferBitSet() {

		resizeBehavior = ResizeBehavior.ALLOCATE;

		buffer = ByteBuffer.allocate(INITIAL_SIZE);
	}

	/**
	 * Creates a {@link BufferBitSet} with the specified resize behavior.
	 * 
	 * @param resizeBehavior - the {@link ResizeBehavior}. If
	 *                       {@link ResizeBehavior#NO_RESIZE} is specified then this
	 *                       bitset will always be empty.
	 * 
	 * @throws NullPointerException if resizeBehavior is null
	 */
	public BufferBitSet(ResizeBehavior resizeBehavior) {

		if (resizeBehavior == null)
			throw new NullPointerException("resizeBehavior cannot be null");

		this.resizeBehavior = resizeBehavior;

		switch (resizeBehavior) {
		case ALLOCATE:
			buffer = ByteBuffer.allocate(INITIAL_SIZE);
			break;
		case ALLOCATE_DIRECT:
			buffer = ByteBuffer.allocateDirect(INITIAL_SIZE);
			break;
		default:
			buffer = ByteBuffer.allocate(0);
			break;
		}
	}

	/**
	 * Creates a {@link BufferBitSet} which wraps the provided buffer. This bitset
	 * will only make use of the space demarked by {@link ByteBuffer#position()} and
	 * {@link ByteBuffer#limit()}. The provided buffer object will not itself be
	 * modified, though of course the buffer's content can be via writes to this
	 * bitset.
	 * <p>
	 * The resize behavior defaults to {@link ResizeBehavior#NO_RESIZE NO_RESIZE}.
	 * 
	 * @param buffer - the {@link ByteBuffer} to be wrapped by this bitset. Writes
	 *               to this bitset will modify the buffer's content.
	 * 
	 * @throws NullPointerException if the provided buffer is null
	 */
	public BufferBitSet(ByteBuffer buffer) {
		this(buffer, NO_RESIZE);
	}

	/**
	 * Creates a {@link BufferBitSet} which wraps the provided buffer. This bitset
	 * will only make use of the space demarked by {@link ByteBuffer#position()} and
	 * {@link ByteBuffer#limit()}. The provided buffer object will not itself be
	 * modified, though of course the buffer's content can be via writes to this
	 * bitset.
	 * 
	 * @param buffer         - the {@link ByteBuffer} to be wrapped by this bitset.
	 *                       Writes to this bitset will modify the buffer's content.
	 * @param resizeBehavior - the {@link ResizeBehavior}
	 * 
	 * @throws NullPointerException if the provided buffer or resizeBehavior is null
	 */
	public BufferBitSet(ByteBuffer buffer, ResizeBehavior resizeBehavior) {
		this(buffer, resizeBehavior, true);
	}

	/**
	 * Returns a new bit set containing all of the bits in the given byte array.
	 * <p>
	 * More precisely, <br>
	 * {@code BufferBitSet.valueOf(bytes).get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)}
	 * <br>
	 * for all {@code n <  8 * bytes.length}.
	 * <p>
	 * <em>The provided array is wrapped, it is not copied.</em> Writes to this
	 * bitset will modify the array.
	 *
	 * @param bytes - a byte array containing a sequence of bits to be used as the
	 *              initial bits of the new bit set
	 * 
	 * @return a {@code BufferBitSet} containing all the bits in the byte array, and
	 *         with resize behavior {@link ResizeBehavior#ALLOCATE ALLOCATE}.
	 */
	public static BufferBitSet valueOf(byte[] bytes) {

		if (bytes.length == 0)
			return new BufferBitSet(ALLOCATE);

		ByteBuffer buffer = ByteBuffer.wrap(bytes);

		// find last set bit
		int n = buffer.limit() - 1;
		while (n >= 0 && buffer.get(n) == 0)
			n--;

		buffer.position(n + 1);

		return new BufferBitSet(buffer, ALLOCATE, false);
	}

	/**
	 * Returns a new {@link BufferBitSet} containing all of the bits in the given
	 * {@link java.util.Bitset}.
	 *
	 * @param bitset - the bitset to copy
	 * 
	 * @return a {@code BufferBitSet} containing all the bits in the given bitset,
	 *         and with resize behavior {@link ResizeBehavior#ALLOCATE ALLOCATE}.
	 */
	public static BufferBitSet valueOf(BitSet bs) {
		byte[] array = bs.toByteArray();
		ByteBuffer buffer = ByteBuffer.wrap(array);
		buffer.limit(array.length);
		buffer.position(array.length);
		return new BufferBitSet(buffer, ALLOCATE, false);
	}

	/**
	 * Returns a new {@link BufferBitSet} with the specified {@link ResizeBehavior
	 * resize} behavior. The buffer object itself will be
	 * {@link ByteBuffer#duplicate duplicated}, but will share the underlying space.
	 * <p>
	 * Concurrent modifications to this bitset and the returned bitset can put both
	 * into a bad state.
	 * 
	 * @param resizeBehavior - the specified {@link ResizeBehavior resize} behavior.
	 */
	public BufferBitSet withResizeBehavior(ResizeBehavior resizeBehavior) {
		return new BufferBitSet(buffer.duplicate(), resizeBehavior, false);
	}

	/*--------------------------------------------------------------------------------
	 *  Methods which export the bits to different formats
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a new byte array containing all the bits in this bit set.
	 */
	public byte[] toByteArray() {

		ByteBuffer buffer = this.buffer.duplicate();
		buffer.flip();

		byte[] array = new byte[buffer.limit()];
		buffer.get(array);

		return array;
	}
	
	/**
	 * Returns a new {@link java.util.BitSet} containing all of the bits in this {@link BufferBitSet}.
	 */
	public BitSet toBitSet() {
		return BitSet.valueOf(toByteArray());
	}

	/*--------------------------------------------------------------------------------
	 *  Get / Set / Flip / Clear
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns the value of the bit with the specified index. The value is
	 * {@code true} if the bit with the index {@code bitIndex} is currently set in
	 * this bitset; otherwise, the result is {@code false}.
	 *
	 * @param bitIndex the bit index
	 * @return the value of the bit with the specified index
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public boolean get(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		return (byteIndex < buffer.position()) && ((buffer.get(byteIndex) & bit(bitIndex)) != 0);
	}

	/**
	 * Returns a new {@code BufferBitSet} composed of bits from this bitset from
	 * {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
	 * <p>
	 * The resulting bitset will have the same {@link ResizeBehavior resize}
	 * behavior as this bitset.
	 *
	 * @param fromIndex - index of the first bit to include
	 * @param toIndex   - index after the last bit to include
	 * @return a new biset from a range of this bitset
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public BufferBitSet get(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		int len = length();

		// If no set bits in range return empty bitset
		if (len <= fromIndex || fromIndex == toIndex)
			return new BufferBitSet(resizeBehavior);

		// An optimization
		if (toIndex > len)
			toIndex = len;

		ByteBuffer resultBuffer = resizeBehavior == ALLOCATE_DIRECT
				? ByteBuffer.allocateDirect((toIndex - fromIndex) * 8)
				: ByteBuffer.allocate((toIndex - fromIndex) * 8);
		BufferBitSet result = new BufferBitSet(resultBuffer, resizeBehavior, false);

		int targetBytes = byteIndex(toIndex - fromIndex - 1) + 1;
		int sourceIndex = byteIndex(fromIndex);
		boolean byteAligned = ((fromIndex & 7) == 0);

		// Process all bytes but the last one
		for (int i = 0; i < targetBytes - 1; i++, sourceIndex++) {
			resultBuffer.put(i,
					(byte) (byteAligned ? buffer.get(sourceIndex)
							: ((buffer.get(sourceIndex) & 0xFF) >>> (fromIndex & 7))
									| (buffer.get(sourceIndex + 1) << ((-fromIndex) & 7))));
		}

		// Process the last byte
		int lastWordMask = MASK >>> ((-toIndex) & 7);
		resultBuffer.put(targetBytes - 1, (byte) (((toIndex - 1) & 7) < (fromIndex & 7) ? /* straddles source bytes */
				(((buffer.get(sourceIndex) & 0xFF) >>> fromIndex)
						| (buffer.get(sourceIndex + 1) & lastWordMask) << ((-fromIndex) & 7))
				: ((buffer.get(sourceIndex) & lastWordMask) >>> (fromIndex & 7))));

		// Set position correctly
		result.buffer.position(targetBytes);
		result.recalculateBytesInUse();

		return result;
	}

	/**
	 * Sets the bit at the specified index to {@code true}.
	 *
	 * @param bitIndex a bit index
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void set(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		expandTo(byteIndex);

		byte b = (byte) (buffer.get(byteIndex) | bit(bitIndex));
		buffer.put(byteIndex, b);
	}

	/**
	 * Sets the bit at the specified index to the specified value.
	 *
	 * @param bitIndex a bit index
	 * @param value    a boolean value to set
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void set(int bitIndex, boolean value) {
		if (value)
			set(bitIndex);
		else
			clear(bitIndex);
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code true}.
	 *
	 * @param fromIndex index of the first bit to be set
	 * @param toIndex   index after the last bit to be set
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void set(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);
		expandTo(endByteIndex);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);
		byte b;
		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			b = (byte) (buffer.get(startByteIndex) | (firstByteMask & lastByteMask));
			buffer.put(startByteIndex, b);
		} else {
			// Case 2: Multiple words
			// Handle first word
			b = (byte) (buffer.get(startByteIndex) | firstByteMask);
			buffer.put(startByteIndex, b);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				buffer.put(i, (byte) MASK);

			// Handle last word
			b = (byte) (buffer.get(endByteIndex) | lastByteMask);
			buffer.put(endByteIndex, b);
		}
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the specified value.
	 *
	 * @param fromIndex index of the first bit to be set
	 * @param toIndex   index after the last bit to be set
	 * @param value     value to set the selected bits to
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void set(int fromIndex, int toIndex, boolean value) {
		if (value)
			set(fromIndex, toIndex);
		else
			clear(fromIndex, toIndex);
	}

	/**
	 * Sets the bit at the specified index to the complement of its current value.
	 *
	 * @param bitIndex the index of the bit to flip
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void flip(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		expandTo(byteIndex);

		byte b = (byte) (buffer.get(byteIndex) ^ bit(bitIndex));
		buffer.put(byteIndex, b);

		recalculateBytesInUse();
	}

	/**
	 * Sets each bit from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the complement of its current value.
	 *
	 * @param fromIndex index of the first bit to flip
	 * @param toIndex   index after the last bit to flip
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void flip(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);
		expandTo(endByteIndex);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);
		byte b;
		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			b = (byte) (buffer.get(startByteIndex) ^ (firstByteMask & lastByteMask));
			buffer.put(startByteIndex, b);
		} else {
			// Case 2: Multiple words
			// Handle first word
			b = (byte) (buffer.get(startByteIndex) ^ firstByteMask);
			buffer.put(startByteIndex, b);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++) {
				b = (byte) (buffer.get(i) ^ MASK);
				buffer.put(i, b);
			}

			// Handle last word
			b = (byte) (buffer.get(endByteIndex) ^ lastByteMask);
			buffer.put(endByteIndex, b);
		}

		recalculateBytesInUse();
	}

	/**
	 * Sets the bit specified by the index to {@code false}.
	 *
	 * @param bitIndex the index of the bit to be cleared
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void clear(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		if (byteIndex >= buffer.position())
			return;

		byte b = (byte) (buffer.get(byteIndex) & ~bit(bitIndex));
		buffer.put(byteIndex, b);

		recalculateBytesInUse();
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code false}.
	 *
	 * @param fromIndex index of the first bit to be cleared
	 * @param toIndex   index after the last bit to be cleared
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void clear(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);
		expandTo(endByteIndex);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);
		byte b;
		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			b = (byte) (buffer.get(startByteIndex) & ~(firstByteMask & lastByteMask));
			buffer.put(startByteIndex, b);
		} else {
			// Case 2: Multiple words
			// Handle first word
			b = (byte) (buffer.get(startByteIndex) & ~firstByteMask);
			buffer.put(startByteIndex, b);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				buffer.put(i, (byte) 0);

			// Handle last word
			b = (byte) (buffer.get(endByteIndex) & ~lastByteMask);
			buffer.put(endByteIndex, b);
		}

		recalculateBytesInUse();
	}

	/*--------------------------------------------------------------------------------
	 *  next/previous set/clear bit
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns the index of the first bit that is set to {@code true} that occurs on
	 * or after the specified starting index. If no such bit exists then {@code -1}
	 * is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next set bit, or {@code -1} if there is no such bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextSetBit(int fromIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return -1;

		byte b = (byte) (buffer.get(u) & (MASK << (fromIndex & 7)));

		while (true) {
			if (b != 0)
				return (u * 8) + Integer.numberOfTrailingZeros(b);
			if (++u == buffer.position())
				return -1;
			b = buffer.get(u);
		}
	}

	/**
	 * Returns the index of the first bit that is set to {@code false} that occurs
	 * on or after the specified starting index.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next clear bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextClearBit(int fromIndex) {

		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return fromIndex;

		byte b = (byte) (~buffer.get(u) & (MASK << (fromIndex & 7)));

		while (true) {
			if (b != 0)
				return (u * 8) + Integer.numberOfTrailingZeros(b);
			if (++u == buffer.position())
				return buffer.position() * 8;
			b = (byte) ~buffer.get(u);
		}
	}

	/**
	 * Returns the index of the nearest bit that is set to {@code true} that occurs
	 * on or before the specified starting index. If no such bit exists, or if
	 * {@code -1} is given as the starting index, then {@code -1} is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous set bit, or {@code -1} if there is no such
	 *         bit
	 * @throws IndexOutOfBoundsException if the specified index is less than
	 *                                   {@code -1}
	 */
	public int previousSetBit(int fromIndex) {
		if (fromIndex < 0) {
			if (fromIndex == -1)
				return -1;
			throw new IndexOutOfBoundsException("fromIndex < -1: " + fromIndex);
		}

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return length() - 1;

		byte b = (byte) (buffer.get(u) & (MASK >>> ((-(fromIndex + 1)) & 7)));

		while (true) {
			if (b != 0)
				return (u + 1) * 8 - 1 - numberOfLeadingZeros(b);
			if (u-- == 0)
				return -1;
			b = buffer.get(u);
		}
	}

	/**
	 * Returns the index of the nearest bit that is set to {@code false} that occurs
	 * on or before the specified starting index. If no such bit exists, or if
	 * {@code -1} is given as the starting index, then {@code -1} is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous clear bit, or {@code -1} if there is no
	 *         such bit
	 * @throws IndexOutOfBoundsException if the specified index is less than
	 *                                   {@code -1}
	 */
	public int previousClearBit(int fromIndex) {
		if (fromIndex < 0) {
			if (fromIndex == -1)
				return -1;
			throw new IndexOutOfBoundsException("fromIndex < -1: " + fromIndex);
		}

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return fromIndex;

		byte b = (byte) (~buffer.get(u) & (MASK >>> ((-(fromIndex + 1)) & 7)));

		while (true) {
			if (b != 0)
				return (u + 1) * 8 - 1 - numberOfLeadingZeros(b);
			if (u-- == 0)
				return -1;
			b = (byte) ~buffer.get(u);
		}
	}

	/*--------------------------------------------------------------------------------
	 *  Logical operations - and/or/xor/andNot
	 *-------------------------------------------------------------------------------*/
	/**
	 * Performs a logical <b>AND</b> of this target bitset with the argument bitset.
	 * This bitset is modified so that each bit in it has the value {@code true} if
	 * and only if it both initially had the value {@code true} and the
	 * corresponding bit in the bitset argument also had the value {@code true}.
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void and(BufferBitSet set) {
		if (this == set)
			return;

		while (buffer.position() > set.buffer.position()) {
			buffer.position(buffer.position() - 1);
			buffer.put(buffer.position(), (byte) 0);
		}

		// Perform logical AND on words in common
		for (int i = 0; i < buffer.position(); i++) {
			byte b = (byte) (buffer.get(i) & set.buffer.get(i));
			buffer.put(i, b);
		}

		recalculateBytesInUse();
	}

	/**
	 * Performs a logical <b>OR</b> of this bitset with the bitset argument. This
	 * bitset is modified so that a bit in it has the value {@code true} if and only
	 * if it either already had the value {@code true} or the corresponding bit in
	 * the bitset argument has the value {@code true}.
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void or(BufferBitSet set) {
		if (this == set)
			return;

		int bytesInCommon = Math.min(this.buffer.position(), set.buffer.position());

		// Perform logical OR on bytes in common
		for (int i = 0; i < bytesInCommon; i++) {
			byte b = (byte) (buffer.get(i) | set.buffer.get(i));
			buffer.put(i, b);
		}

		copyRemainingBytes(bytesInCommon, set);

		// recalculateBytesInUse() is unnecessary
	}

	/**
	 * Performs a logical <b>XOR</b> of this bitset with the bitset argument. This
	 * bitset is modified so that a bit in it has the value {@code true} if and only
	 * if one of the following statements holds:
	 * <ul>
	 * <li>The bit initially has the value {@code true}, and the corresponding bit
	 * in the argument has the value {@code false}.
	 * <li>The bit initially has the value {@code false}, and the corresponding bit
	 * in the argument has the value {@code true}.
	 * </ul>
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void xor(BufferBitSet set) {

		int bytesInCommon = Math.min(this.buffer.position(), set.buffer.position());

		// Perform logical XOR on bytes in common
		for (int i = 0; i < bytesInCommon; i++) {
			byte b = (byte) (buffer.get(i) ^ set.buffer.get(i));
			buffer.put(i, b);
		}

		copyRemainingBytes(bytesInCommon, set);

		recalculateBytesInUse();
	}

	/**
	 * Clears all of the bits in this bitset whose corresponding bit is set in the
	 * specified bitset.
	 *
	 * @param set - the {@link BufferBitSet} with which to mask this bitset
	 */
	public void andNot(BufferBitSet set) {
		// Perform logical (a & !b) on bytes in common
		for (int i = Math.min(buffer.position(), set.buffer.position()) - 1; i >= 0; i--) {
			byte b = (byte) (buffer.get(i) & ~set.buffer.get(i));
			buffer.put(i, b);
		}

		recalculateBytesInUse();
	}

	/*--------------------------------------------------------------------------------
	 *  Object & Collection-like methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a string representation of this {@link BufferBitSet} equivalent to
	 * the representation of a {@code SortedSet} containing the indices of the bits
	 * which are set in this bitset.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');

		for (int i = 0; i < buffer.position(); i++) {
			int b = buffer.get(i) & 0xFF;
			for (int j = 0; b > 0; b >>= 1, j++) {
				if ((b & 1) != 0) {
					sb.append((i << 3) + j);
					sb.append(", ");
				}
			}
		}

		if (sb.length() > 1)
			sb.delete(sb.length() - 2, sb.length());

		sb.append(']');
		return sb.toString();
	}

	/**
	 * Returns the "logical size" of this bitset: the index of the highest set bit
	 * in the bitset plus one. Returns zero if the bitset contains no set bits.
	 *
	 * @return the logical size of this biset
	 */
	public int length() {
		if (buffer.position() == 0)
			return 0;

		return 8 * (buffer.position() - 1) + (8 - numberOfLeadingZeros(buffer.get(buffer.position() - 1)));
	}

	/**
	 * Returns the number of bits of space actually in use by this
	 * {@link BufferBitSet} to represent bit values. The maximum element that can be
	 * set without resizing is {@code size()-1}
	 *
	 * @return the number of bits of space currently in this bit set
	 */
	public int size() {
		return buffer.limit() * 8;
	}

	/**
	 * Returns true if this {@link BufferBitSet} contains no bits that are set to
	 * {@code true}.
	 *
	 * @return boolean indicating whether this bitset is empty
	 */
	public boolean isEmpty() {
		return buffer.position() == 0;
	}

	/**
	 * Returns the number of bits set to {@code true} in this {@link BufferBitSet}.
	 *
	 * @return the number of bits set to {@code true} in this {@link BufferBitSet}
	 */
	public int cardinality() {
		int sum = 0;
		for (int i = 0; i < buffer.position(); i++)
			sum += Integer.bitCount(buffer.get(i) & 0xFF);
		return sum;
	}

	/**
	 * Returns the hashcode value for this bitset. The hashcode depends only on
	 * which bits are set within this {@link BufferBitSet}.
	 * <p>
	 * Hashcode is computed using formula from
	 * {@link java.util.Arrays#hashCode(byte[])}
	 *
	 * @return the hashcode value for this bitset
	 */
	@Override
	public int hashCode() {
		// borrowed from Arrays#hashCode(byte a[])
		if (isEmpty())
			return 0;

		int result = 1;
		for (int i = 0; i < buffer.position(); i++)
			result = 31 * result + buffer.get(i);

		return result;
	}

	/*--------------------------------------------------------------------------------
	 *  Methods related to resizing
	 *-------------------------------------------------------------------------------*/
	/**
	 * Ensures that the BitSet can accommodate a given wordIndex.
	 */
	private void expandTo(int byteIndex) {

		if (byteIndex >= buffer.limit()) {
			if (resizeBehavior == NO_RESIZE)
				throw new IndexOutOfBoundsException("could not resize to accomodate byte index: " + byteIndex);

			// allocate new buffer with twice as much space
			final int capacity = Math.max(buffer.limit() * 2, byteIndex + 1);
			final ByteBuffer buffer;

			if (resizeBehavior == ALLOCATE)
				buffer = ByteBuffer.allocate(capacity);
			else
				buffer = ByteBuffer.allocateDirect(capacity);

			// copy old buffer and replace with new one
			this.buffer.flip();
			this.buffer = buffer.put(this.buffer);
		}

		if (byteIndex >= buffer.position())
			buffer.position(byteIndex + 1);
	}

	private void recalculateBytesInUse() {
		// find last set bit
		int n = buffer.position() - 1;
		while (n >= 0 && buffer.get(n) == 0)
			n--;

		buffer.position(n + 1);
	}

	private void copyRemainingBytes(int bytesInCommon, BufferBitSet set) {
		// Copy any remaining bytes
		if (bytesInCommon < set.buffer.position()) {
			expandTo(set.buffer.position() - 1);
			buffer.position(bytesInCommon);

			ByteBuffer remaining = set.buffer.duplicate();
			remaining.position(bytesInCommon);
			remaining.limit(set.buffer.position());

			buffer.put(remaining);
		}
	}

	/*--------------------------------------------------------------------------------
	 *  Utility methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Given a bit index, return byte index containing it.
	 */
	private static int byteIndex(int bitIndex) {
		return bitIndex >> 3;
	}

	private static int bit(int bitIndex) {
		return 1 << (bitIndex & 7);
	}

	/**
	 * Checks that fromIndex ... toIndex is a valid range of bit indices.
	 */
	private static void checkRange(int fromIndex, int toIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
		if (toIndex < 0)
			throw new IndexOutOfBoundsException("toIndex < 0: " + toIndex);
		if (fromIndex > toIndex)
			throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + " > toIndex: " + toIndex);
	}

	private static int numberOfLeadingZeros(byte b) {

		return Integer.numberOfLeadingZeros(b & 0xFF) & 7;
	}
}
