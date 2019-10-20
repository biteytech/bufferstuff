/*-
 *  Copyright 2019 Lior Privman
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.privman.lior.bytebufferbitset;

import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.util.SortedSet;

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
 * 
 * @see java.util.BitSet
 * @see java.nio.ByteBuffer
 */
public class BufferBitSet {

	private static final int INITIAL_SIZE = 8;

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

		if(external) {
			this.buffer = buffer.slice();
			
			// TODO: zero-out as we go (tracking highwater mark)
			byte zero = (byte)0;
			for(int i = 0; i < buffer.limit(); i++)
				this.buffer.put(i, zero);
		}
		else {
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
     * @param  bitIndex a bit index
     * @param  value a boolean value to set
     * @throws IndexOutOfBoundsException if the specified index is negative
     */
    public void set(int bitIndex, boolean value) {
        if (value)
            set(bitIndex);
        else
            clear(bitIndex);
    }
	
    /**
     * Sets the bit at the specified index to the complement of its
     * current value.
     *
     * @param  bitIndex the index of the bit to flip
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
     * Sets the bit specified by the index to {@code false}.
     *
     * @param  bitIndex the index of the bit to be cleared
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

	/*--------------------------------------------------------------------------------
	 *  Object methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a string representation of this {@link BufferBitSet} equivalent to
	 * the representation of a {@link SortedSet} containing the indices of the bits
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
}
