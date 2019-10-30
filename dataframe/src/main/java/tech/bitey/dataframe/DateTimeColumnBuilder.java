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
import java.time.LocalDateTime;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;

public class DateTimeColumnBuilder extends LongArrayColumnBuilder<LocalDateTime, DateTimeColumn, DateTimeColumnBuilder> {

	DateTimeColumnBuilder(int characteristics) {
		super(characteristics, LongArrayPacker.LOCAL_DATE_TIME);
	}

	@Override
	DateTimeColumn emptyNonNull() {
		return NonNullDateTimeColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	DateTimeColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullDateTimeColumn(trim, 0, getNonNullSize(), characteristics);
	}

	@Override
	DateTimeColumn wrapNullableColumn(DateTimeColumn column, BufferBitSet nonNulls) {
		return new NullableDateTimeColumn((NonNullDateTimeColumn)column, nonNulls, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.DATETIME;
	}
}
