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

import java.nio.ByteBuffer;
import java.time.LocalDate;

import tech.bitey.bufferstuff.BufferBitSet;

public class DateColumnBuilder extends IntArrayColumnBuilder<LocalDate, DateColumn, DateColumnBuilder> {

	protected DateColumnBuilder(int characteristics) {
		super(characteristics, IntArrayPacker.LOCAL_DATE);
	}

	@Override
	protected DateColumn empty() {
		return NonNullDateColumn.EMPTY.get(characteristics);
	}

	@Override
	protected DateColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullDateColumn(trim, 0, getNonNullSize(), characteristics);
	}

	@Override
	protected DateColumn wrapNullableColumn(DateColumn column, BufferBitSet nonNulls) {
		return new NullableDateColumn((NonNullDateColumn)column, nonNulls, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.DATE;
	}
	
	public DateColumnBuilder add(int year, int month, int day) {
		ensureAdditionalCapacity(1);
		elements.put(IntArrayPacker.packDate(year, month, day));
		size++;
		return this;
	}
}
