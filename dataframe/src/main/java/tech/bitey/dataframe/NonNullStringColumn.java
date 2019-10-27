/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe;

import static java.lang.Math.max;
import static tech.bitey.bufferstuff.BufferUtils.duplicate;
import static tech.bitey.bufferstuff.BufferUtils.slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Comparator;

import org.eclipse.collections.api.list.primitive.MutableIntList;

import tech.bitey.bufferstuff.BufferBitSet;

class NonNullStringColumn extends NonNullColumn<String, NonNullStringColumn> implements StringColumn {

	private static ByteBuffer allocate(int capacity) {
		return ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
	}
	
	static final NonNullStringColumn EMPTY_LIST =
			new NonNullStringColumn(allocate(0), allocate(0), 0, 0, false);
	static final NonNullStringColumn EMPTY_SET =
			new NonNullStringColumn(allocate(0), allocate(0), 0, 0, true);
	
	private final ByteBuffer elements;
	
	private final ByteBuffer rawPointers;
	private final IntBuffer pointers; // pointers[0] is always 0 - it's just easier that way :P
	
	NonNullStringColumn(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size, boolean sortedSet) {
		super(offset, size, sortedSet);
		
		validateBuffer(elements);
		validateBuffer(rawPointers);
		
		this.elements = elements;
		
		this.rawPointers = rawPointers;
		this.pointers = rawPointers.asIntBuffer();
	}
	
	// pointer at index
	private int pat(int index) {
		return pointers.get(index);
	}
	
	// element byte at index
	private int bat(int index) {
		return elements.get(index);
	}
	
	@Override
	protected NonNullStringColumn toHeap0() {
		return new NonNullStringColumn(elements, rawPointers, offset, size, false);
	}
	
	private int end(int index) {
		return index == pointers.limit()-1 ? elements.limit() : pat(index+1);
	}
	
	private int length(int index) {
		return end(index) - pat(index);
	}
	
	@Override
	protected String getNoOffset(int index) {
		
		ByteBuffer element = duplicate(elements);
		element.position(pat(index));
		element.limit(end(index));

		byte[] bytes = new byte[length(index)];
		element.get(bytes);
		
		return new String(bytes, UTF_8);
	}

	@Override
	protected NonNullStringColumn subColumn0(int fromIndex, int toIndex) {
		return new NonNullStringColumn(elements, rawPointers, fromIndex+offset, toIndex-fromIndex, sortedSet);
	}

	@Override
	protected NonNullStringColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	protected int search(String value) {
		return binarySearch(offset, offset+size, value);
	}
	
	@Override	
	protected int search(String value, boolean first) {
		if(sortedSet)
			return search(value);
		else
			return indexOf(value, first) + offset;
	}
	
	/**
	 * Searches the specified value using the binary search algorithm.
	 * <p>
	 * Adapted from {@code Arrays::binarySearch0}
	 *
	 * @param key the value to be searched for
	 * @return index of the specified key, if it is contained in this column; otherwise,
	 *         <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion point</i>
	 *         is defined as the point at which the key would be inserted into the
	 *         array: the index of the first element greater than the key, or
	 *         <tt>a.length</tt> if all elements in the array are less than the
	 *         specified key. Note that this guarantees that the return value will
	 *         be &gt;= 0 if and only if the key is found.
	 */
	private int binarySearch(int fromIndex, int toIndex, String key) {
		
		int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
		
            String midVal = getNoOffset(mid);
            
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
	}
	
	@Override
	public Comparator<String> comparator() {
		return String::compareTo;
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.STRING;
	}
	
	@Override
	public int hashCode(int fromIndex, int toIndex) {		
		// from Arrays::hashCode
		int result = 1;
        
		for (int i = fromIndex; i <= toIndex; i++)
            result = 31 * result + length(i);
        
		for (int i = pat(fromIndex); i < end(toIndex); i++)
            result = 31 * result + bat(i);
		
        return result;
	}

	@Override
	protected boolean equals0(NonNullStringColumn rhs, int lStart, int rStart, int length) {
		
		for(int i = 0; i < length; i++)
			if(length(lStart+i) != rhs.length(rStart+i))
				return false;
		
		final int end = end(lStart+length-1);
		for(int i = pat(lStart), j = rhs.pat(rStart); i < end; i++, j++)
			if(bat(i) != rhs.bat(j))
				return false;
		
		return true;
	}

	private void copyElement(int i, ByteBuffer dest) {
		ByteBuffer src = slice(elements, pat(i), end(i));
		dest.put(src);
	}
	
	@Override
	protected NonNullStringColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		ByteBuffer rawPointers = allocate(cardinality*4);
		int byteLength = 0;		
		for(int i = offset; i <= lastIndex(); i++) {
			if(keep.get(i - offset)) {
				rawPointers.putInt(byteLength);
				byteLength += length(i);
			}
		}
		rawPointers.flip();
		
		ByteBuffer elements = allocate(byteLength);
		for(int i = offset; i <= lastIndex(); i++)
			if(keep.get(i - offset))
				copyElement(i, elements);
		elements.flip();
		
		return new NonNullStringColumn(elements, rawPointers, 0, cardinality, sortedSet);
	}

	@Override
	protected NonNullStringColumn select0(int[] indices) {
		
		ByteBuffer rawPointers = allocate(indices.length*4);
		int byteLength = 0;		
		for(int i = 0; i < indices.length; i++) {
			rawPointers.putInt(byteLength);
			byteLength += length(indices[i] + offset);
		}
		rawPointers.flip();
		
		ByteBuffer elements = allocate(byteLength);
		for(int i = 0; i < indices.length; i++) {
			int index = indices[i] + offset;
			copyElement(index, elements);
		}
		elements.flip();
		
		return new NonNullStringColumn(elements, rawPointers, 0, indices.length, false);
	}
	
	@Override
	protected NonNullStringColumn appendNonNull(NonNullStringColumn tail) {
		
		final int thisByteLength = this.end(this.lastIndex()) - this.pat(this.offset);
		final int tailByteLength = tail.end(tail.lastIndex()) - tail.pat(tail.offset);
		
		ByteBuffer elements = allocate(thisByteLength + tailByteLength);
		{
			ByteBuffer thisElements = slice(this.elements, this.pat(this.offset), this.end(this.lastIndex()));
			ByteBuffer tailElements = slice(tail.elements, tail.pat(tail.offset), tail.end(tail.lastIndex()));
			
			elements.put(thisElements);
			elements.put(tailElements);
			elements.flip();
		}
		
		ByteBuffer rawPointers = allocate((size() + tail.size())*4);
		{
			ByteBuffer thisPointers = slice(this.rawPointers, this.offset*4, (this.offset+this.size())*4);
			ByteBuffer tailPointers = slice(tail.rawPointers, tail.offset*4, (tail.offset+tail.size())*4);
			
			rawPointers.put(thisPointers);
			rawPointers.put(tailPointers);
			rawPointers.flip();
		}
				
		final IntBuffer pointers = rawPointers.asIntBuffer();
		final int size = pointers.limit();
		for(int i = this.size(); i < size; i++)
			pointers.put(i, pointers.get(i) + thisByteLength);
			
		return new NonNullStringColumn(elements, rawPointers, 0, size, sortedSet);
	}

	@Override
	protected int compareValuesAt(NonNullStringColumn rhs, int l, int r) {
		return getNoOffset(l+offset).compareTo(rhs.getNoOffset(r+rhs.offset));
	}

	@Override
	protected void intersectLeftSorted(NonNullStringColumn rhs, MutableIntList indices, BufferBitSet keepRight) {
		
		for(int i = rhs.offset; i <= rhs.lastIndex(); i++) {
			
			int leftIndex = search(rhs.getNoOffset(i));			
			if(leftIndex >= offset && leftIndex <= lastIndex()) {
				
				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	protected String oracleType() {
		
		if(isEmpty())
			return "VARCHAR2(255)";
		
		int maxLength = 0;
		for(int i = offset; i <= lastIndex(); i++)
			maxLength = max(maxLength, length(i));
		
		int padding = (int)max(1, maxLength * 0.1);
		
		return "VARCHAR2("+(maxLength+padding)+")";
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	ByteOrder byteOrder() {
		return pointers.order();
	}

	@Override
	int byteLength() {
		return 4 + size() * 4 + end(lastIndex()) - pat(offset);
	}

	@Override
	ByteBuffer[] asBuffers() {
		ByteBuffer[] buffers =  new ByteBuffer[3];
		
		buffers[0] = ByteBuffer.allocate(4).order(byteOrder());
		buffers[0].putInt(size());
		buffers[0].flip();
		
		buffers[1] = slice(rawPointers, offset*4, (offset+size)*4);		
		buffers[2] = slice(elements, pat(offset), end(lastIndex()));
		
		return buffers;
	}
	
	static NonNullStringColumn fromBuffer(ByteBuffer buffer, int offset, int length, boolean isSorted) {
		final int size = buffer.getInt(offset);
		
		ByteBuffer rawPointers = slice(buffer, offset + 4, offset + 4 + size*4);
		
		if(size > 0) {
			IntBuffer pointers = rawPointers.asIntBuffer();
			int first = pointers.get(0);
			for(int i = 0; i < size; i++)
				pointers.put(i, pointers.get(i) - first);
		}
		
		ByteBuffer elements = slice(buffer, offset + 4 + size*4, offset+length);
		
		return new NonNullStringColumn(elements, rawPointers, 0, size, isSorted);
	}
}
