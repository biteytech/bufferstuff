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
	 * of the current buffer's space will throw a {@link IndexOutOfBoundsException}.
	 */
	NO_RESIZE,

	/**
	 * Additional storage will be allocated using {@link ByteBuffer#allocate(int)}.
	 * <p>
	 * <b>Note</b>: <em>If an external {@link ByteBuffer} was supplied, or an
	 * interal buffer unwrapped, they will no longer be updated by future writes to
	 * this bitset after a resize.</em>
	 */
	ALLOCATE,

	/**
	 * Additional storage will be allocated using
	 * {@link ByteBuffer#allocateDirect(int)}.
	 * <p>
	 * <b>Note</b>: <em>If an external {@link ByteBuffer} was supplied, or an
	 * interal buffer unwrapped, they will no longer be updated by future writes to
	 * this bitset after a resize.</em>
	 */
	ALLOCATE_DIRECT,
}
