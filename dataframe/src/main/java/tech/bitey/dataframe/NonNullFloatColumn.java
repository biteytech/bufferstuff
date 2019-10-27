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

import static java.lang.Math.abs;
import static java.lang.Math.log10;
import static java.lang.Math.max;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.Comparator;

import org.eclipse.collections.api.list.primitive.MutableIntList;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;

class NonNullFloatColumn extends NonNullSingleBufferColumn<Float, NonNullFloatColumn> implements FloatColumn {

	static final NonNullFloatColumn EMPTY_LIST = new NonNullFloatColumn(ByteBuffer.allocate(0), 0, 0, false);
	static final NonNullFloatColumn EMPTY_SET  = new NonNullFloatColumn(ByteBuffer.allocate(0), 0, 0, true);
	
	private final FloatBuffer elements;
	
	NonNullFloatColumn(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		super(buffer, offset, size, sortedSet);
		
		this.elements = buffer.asFloatBuffer();
	}
	
	@Override
	NonNullFloatColumn construct(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		return new NonNullFloatColumn(buffer, offset, size, sortedSet);
	}
	
	private float at(int index) {
		return elements.get(index);
	}
	
	@Override
	protected Float getNoOffset(int index) {
		return at(index);
	}

	@Override
	public double mean() {
		if(size == 0)
        	return Double.NaN;
		
		double sum = 0;
		for(int i = offset; i < size; i++)
			sum += at(i);

        // Compute initial estimate using definitional formula
        double xbar = sum / size;

        // Compute correction factor in second pass
        double correction = 0;
        for (int i = offset; i < size; i++) {
            correction += at(i) - xbar;
        }
        return xbar + (correction/size);
	}

	private int search(float value) {
		return binarySearch(offset, offset+size, value); 
	}
	
	private int binarySearch(int fromIndex, int toIndex, float key) {
		return BufferSearch.binarySearch(elements, fromIndex, toIndex, key);		
	}
	
	@Override
	protected int search(Float value, boolean first) {
		if(sortedSet)
			return search(value);
		else {
			float d = value;
			
			if(first) {
				for(int i = offset; i <= lastIndex(); i++)
					if(Float.compare(at(i), d) == 0)
						return i;
			}
			else {
				for(int i = lastIndex(); i >= offset; i--)
					if(Float.compare(at(i), d) == 0)
						return i;
			}
			
			return -1;
		}
	}

	@Override
	protected NonNullFloatColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	@Override
	public Comparator<Float> comparator() {
		return Float::compareTo;
	}
	
	@Override
	public float getFloat(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.FLOAT;
	}
	
	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {
			int bits = Float.floatToIntBits(at(i));
			result = 31 * result + bits;
		}
		return result;
	}

	@Override
	protected boolean equals0(NonNullFloatColumn rhs, int lStart, int rStart, int length) {
		for(int i = 0; i < length; i++)
			if(Float.compare(at(lStart+i), rhs.at(rStart+i)) != 0)
				return false;
		return true;
	}

	@Override
	protected NonNullFloatColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		ByteBuffer buffer = allocate(cardinality);
		for(int i = offset; i <= lastIndex(); i++)
			if(keep.get(i - offset))
				buffer.putFloat(at(i));
		buffer.flip();
		
		return new NonNullFloatColumn(buffer, 0, cardinality, sortedSet);
	}

	@Override
	protected NonNullFloatColumn select0(int[] indices) {
		
		ByteBuffer buffer = allocate(indices.length);
		for(int i = 0; i < indices.length; i++)
			buffer.putFloat(at(indices[i]+offset));
		buffer.flip();
		
		return new NonNullFloatColumn(buffer, 0, indices.length, false);
	}

	@Override
	protected int compareValuesAt(NonNullFloatColumn rhs, int l, int r) {
		return Float.compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	protected void intersectLeftSorted(NonNullFloatColumn rhs, MutableIntList indices, BufferBitSet keepRight) {
		
		for(int i = rhs.offset; i <= rhs.lastIndex(); i++) {
			
			int leftIndex = search(rhs.at(i));			
			if(leftIndex >= offset && leftIndex <= lastIndex()) {
				
				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	protected String oracleType() {
		
		if(isEmpty())
			return "NUMBER(18,6)";
		
		int maxLength = 0;
		for(int i = offset; i <= lastIndex(); i++)
			maxLength = max(maxLength, (int)log10(abs(at(i))+1));
		
		return "NUMBER("+(maxLength+8)+",6)";
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Float;
	}

	@Override
	int elementSize() {
		return 4;
	}
}
