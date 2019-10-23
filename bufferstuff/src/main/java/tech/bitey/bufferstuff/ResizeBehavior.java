package tech.bitey.bufferstuff;

import java.nio.ByteBuffer;

/**
 * Specifies the resize behavior of a {@link BufferBitSet}. One of:
 * <ul>
 * <li>{@link #NO_RESIZE}
 * <li>{@link #ALLOCATE}
 * <li>{@link #ALLOCATE_DIRECT}
 * </ul>
 * 
 * @author Lior Privman
 */
public enum ResizeBehavior {

	/**
	 * Additional storage will not be allocated. Attempting to write a bit outside
	 * of the current buffer's space will throw an
	 * {@link IndexOutOfBoundsException}.
	 */
	NO_RESIZE,

	/**
	 * Additional storage will be allocated using {@link ByteBuffer#allocate(int)}.
	 * <p>
	 * <b>Note</b>: <em>If an external {@link ByteBuffer} was supplied, or an
	 * internal buffer unwrapped, they will no longer be updated by future writes to
	 * this bitset after a resize.</em>
	 */
	ALLOCATE,

	/**
	 * Additional storage will be allocated using
	 * {@link ByteBuffer#allocateDirect(int)}.
	 * <p>
	 * <b>Note</b>: <em>If an external {@link ByteBuffer} was supplied, or an
	 * internal buffer unwrapped, they will no longer be updated by future writes to
	 * this bitset after a resize.</em>
	 */
	ALLOCATE_DIRECT,
}
