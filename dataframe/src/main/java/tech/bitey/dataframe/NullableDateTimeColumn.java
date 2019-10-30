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

import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.time.LocalDateTime;

import tech.bitey.bufferstuff.BufferBitSet;

class NullableDateTimeColumn extends NullableColumn<LocalDateTime, DateTimeColumn, NonNullDateTimeColumn, NullableDateTimeColumn> implements DateTimeColumn {
	
	static final NullableDateTimeColumn EMPTY = new NullableDateTimeColumn(NonNullDateTimeColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_NO_RESIZE, 0, 0);

	NullableDateTimeColumn(NonNullDateTimeColumn column, BufferBitSet nonNulls, int offset, int size) {
		super(column, nonNulls, offset, size);
	}

	@Override
	NullableDateTimeColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableDateTimeColumn(column, nonNulls, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	NullableDateTimeColumn empty() {
		return EMPTY;
	}

	@Override
	NullableDateTimeColumn construct(NonNullDateTimeColumn column, BufferBitSet nonNulls, int size) {
		return new NullableDateTimeColumn(column, nonNulls, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof LocalDateTime;
	}
}
