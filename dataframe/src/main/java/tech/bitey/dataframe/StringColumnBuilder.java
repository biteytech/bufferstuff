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

import static tech.bitey.dataframe.NonNullStringColumn.EMPTY_LIST;
import static tech.bitey.dataframe.NonNullStringColumn.EMPTY_SET;
import static tech.bitey.dataframe.StringColumn.UTF_8;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import tech.bitey.bufferstuff.BufferBitSet;

public class StringColumnBuilder extends ColumnBuilder<String, StringColumn, StringColumnBuilder> {

	protected StringColumnBuilder(boolean sortedSet) {
		super(sortedSet);
	}

	private final ArrayList<String> elements = new ArrayList<>();

	@Override
	protected void addNonNull(String element) {		
		elements.add(element);
		size++;
	}

	@Override
	protected void ensureAdditionalCapacity(int size) {
		elements.ensureCapacity(elements.size() + size);
	}

	@Override
	protected StringColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}

	@Override
	protected int getNonNullSize() {
		return elements.size();
	}

	@Override
	protected void checkSortedAndDistinct() {
		if(elements.size() >= 2) {
			String prev = elements.get(0);
			for(int i = 1; i < elements.size(); i++) {
				String e = elements.get(i);
				checkArgument(prev.compareTo(e) < 0, "column elements are not sorted and distinct");
				prev = e;
			}
		}
	}

	@Override
	protected StringColumn buildNonNullColumn() {
		
		int byteLength = 0;
		for(int i = 0; i < elements.size(); i++)
			byteLength += elements.get(i).getBytes(UTF_8).length;
		
		ByteBuffer elements = ByteBuffer.allocateDirect(byteLength);
		ByteBuffer pointers = ByteBuffer.allocateDirect(this.elements.size()*4).order(ByteOrder.nativeOrder());
		
		int destPos = 0;
		for(String e : this.elements) {
			byte[] bytes = e.getBytes(UTF_8);
			elements.put(bytes);
			pointers.putInt(destPos);
			destPos += bytes.length;
		}
		pointers.flip();
		elements.flip();
	
		return new NonNullStringColumn(elements, pointers, 0, this.elements.size(), sortedSet);
	}

	@Override
	protected StringColumn wrapNullableColumn(StringColumn column, BufferBitSet nonNulls) {
		
		NullableStringColumn nullable = new NullableStringColumn((NonNullStringColumn)column, nonNulls, 0, size);
		return nullable;
	}

	@Override
	public ColumnType getType() {
		return ColumnType.STRING;
	}

	@Override
	protected void append0(StringColumnBuilder tail) {
		this.elements.addAll(tail.elements);
	}
}
