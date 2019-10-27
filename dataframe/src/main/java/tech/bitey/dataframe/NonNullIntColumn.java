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
import static tech.bitey.dataframe.IntArrayPacker.INTEGER;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.util.Comparator;

class NonNullIntColumn extends IntArrayColumn<Integer, NonNullIntColumn> implements IntColumn {

	static final NonNullIntColumn EMPTY_LIST = new NonNullIntColumn(ByteBuffer.allocate(0), 0, 0, false);
	static final NonNullIntColumn EMPTY_SET = new NonNullIntColumn(ByteBuffer.allocate(0), 0, 0, false);
	
	NonNullIntColumn(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		super(buffer, INTEGER, offset, size, sortedSet);
	}

	@Override
	NonNullIntColumn construct(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		return new NonNullIntColumn(buffer, offset, size, sortedSet);
	}

	@Override
	public double mean() {
		long sum = 0;
		for(int i = 0; i < size; i++)
			sum += at(i+offset);
		return sum / (double)size;
	}

	@Override
	protected NonNullIntColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	@Override
	public Comparator<Integer> comparator() {
		return Integer::compareTo;
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.INT;
	}

	@Override
	public int getInt(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}

	@Override
	protected String oracleType() {
		
		if(isEmpty())
			return "NUMBER(10)";
		
		int maxLength = 0;
		for(int i = offset; i <= lastIndex(); i++)
			maxLength = max(maxLength, (int)log10(abs(at(i))+1));
		
		return "NUMBER("+(maxLength+2)+")";
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Integer;
	}
}
