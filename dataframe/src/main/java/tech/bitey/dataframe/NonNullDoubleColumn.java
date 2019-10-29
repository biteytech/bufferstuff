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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.collections.api.list.primitive.MutableIntList;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;

class NonNullDoubleColumn extends NonNullSingleBufferColumn<Double, NonNullDoubleColumn> implements DoubleColumn {
	
	static final Map<Integer, NonNullDoubleColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullDoubleColumn(ByteBuffer.allocate(0), 0, 0, c));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED, c -> new NonNullDoubleColumn(ByteBuffer.allocate(0), 0, 0, c));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT, c -> new NonNullDoubleColumn(ByteBuffer.allocate(0), 0, 0, c));
	}
	
	private final DoubleBuffer elements;
	
	NonNullDoubleColumn(ByteBuffer buffer, int offset, int size, int characteristics) {
		super(buffer, offset, size, characteristics);
		
		this.elements = buffer.asDoubleBuffer();
	}
	
	@Override
	NonNullDoubleColumn construct(ByteBuffer buffer, int offset, int size, int characteristics) {
		return new NonNullDoubleColumn(buffer, offset, size, characteristics);
	}
	
	private double at(int index) {
		return elements.get(index);
	}
	
	@Override
	Double getNoOffset(int index) {
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

	private int search(double value) {
		return BufferSearch.binarySearch(elements, offset, offset+size, value);
	}
	
	@Override
	int search(Double value, boolean first) {
		if(isSorted()) {
			int index = search(value);
			if(isDistinct() || index < 0)
				return index;			
			else if(first)
				return BufferSearch.binaryFindFirst(elements, offset, index);
			else
				return BufferSearch.binaryFindLast(elements, offset+size, index);
		}
		else {
			double d = value;
			
			if(first) {
				for(int i = offset; i <= lastIndex(); i++)
					if(Double.compare(at(i), d) == 0)
						return i;
			}
			else {
				for(int i = lastIndex(); i >= offset; i--)
					if(Double.compare(at(i), d) == 0)
						return i;
			}
			
			return -1;
		}
	}

	@Override
	NonNullDoubleColumn empty() {
		return EMPTY.get(characteristics);
	}
	
	@Override
	public Comparator<Double> comparator() {
		return Double::compareTo;
	}
	
	@Override
	public double getDouble(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.DOUBLE;
	}
	
	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {
			long bits = Double.doubleToLongBits(at(i));
			result = 31 * result + (int) (bits ^ (bits >>> 32));
		}
		return result;
	}

	@Override
	NonNullDoubleColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		ByteBuffer buffer = allocate(cardinality);
		for(int i = offset; i <= lastIndex(); i++)
			if(keep.get(i - offset))
				buffer.putDouble(at(i));
		buffer.flip();
		
		return new NonNullDoubleColumn(buffer, 0, cardinality, characteristics);
	}

	@Override
	NonNullDoubleColumn select0(int[] indices) {
		
		ByteBuffer buffer = allocate(indices.length);
		for(int i = 0; i < indices.length; i++)
			buffer.putDouble(at(indices[i]+offset));
		buffer.flip();
		
		return new NonNullDoubleColumn(buffer, 0, indices.length, NONNULL);
	}

	@Override
	int compareValuesAt(NonNullDoubleColumn rhs, int l, int r) {
		return Double.compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(NonNullDoubleColumn rhs, MutableIntList indices, BufferBitSet keepRight) {
		
		for(int i = rhs.offset; i <= rhs.lastIndex(); i++) {
			
			int leftIndex = search(rhs.at(i));			
			if(leftIndex >= offset && leftIndex <= lastIndex()) {
				
				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Double;
	}

	@Override
	int elementSize() {
		return 8;
	}
}
