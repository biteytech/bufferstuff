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

import static tech.bitey.bufferstuff.BufferUtils.slice;
import static tech.bitey.dataframe.AbstractColumn.readBufferBitSet;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.nio.ByteBuffer;

import tech.bitey.dataframe.AbstractColumn.BufferBitSetWrapper;

public enum ColumnType {

	BOOLEAN("B") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			BufferBitSetWrapper wrapper = readBufferBitSet(slice(buffer, offset, offset+length));
			return new NonNullBooleanColumn(wrapper.bbs, wrapper.offset, wrapper.size);
		}
	},
	DATE("DA") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullDateColumn(slice(buffer, offset, offset+length), 0, (length - offset)/4, characteristics);
		}
	},
	DATETIME("DT") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullDateTimeColumn(slice(buffer, offset, offset+length), 0, (length - offset)/8, characteristics);
		}
	},
	DOUBLE("D") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullDoubleColumn(slice(buffer, offset, offset+length), 0, (length - offset)/8, characteristics);
		}
	},
	FLOAT("F") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullFloatColumn(slice(buffer, offset, offset+length), 0, (length - offset)/4, characteristics);
		}
	},
	INT("I") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullIntColumn(slice(buffer, offset, offset+length), 0, (length - offset)/4, characteristics);
		}
	},
	LONG("L") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return new NonNullLongColumn(slice(buffer, offset, offset+length), 0, (length - offset)/8, characteristics);
		}
	},
	STRING("S") {
		@Override
		Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics) {
			return NonNullStringColumn.fromBuffer(buffer, offset, length, characteristics);
		}
	},
	;
	
	private final String code;
	
	private ColumnType(String code) {
		
		byte[] codeBytes = code.getBytes();
		checkState(codeBytes.length >= 1 && codeBytes.length <= 2,
				"code must be one or two (ascii) characters");
		
		this.code = code;
	}
	
	public String getCode() {
		return code;
	}
	
	byte[] getCodeBytes() {
		return code.length() == 2 ? code.getBytes() : (" "+code).getBytes();
	}
	
	static ColumnType valueOf(byte[] codeBytes) {
		final String code;
		if(codeBytes[0] == ' ')
			code = String.valueOf((char)codeBytes[1]);
		else
			code = new String(codeBytes);
		
		switch(code) {
		case "B": return BOOLEAN;
		case "DA": return DATE;
		case "DT": return DATETIME;
		case "D": return DOUBLE;
		case "F": return FLOAT;
		case "I": return INT;
		case "L": return LONG;
		case "S": return STRING;
		default:
			throw new IllegalArgumentException("bad code bytes: "+code);
		}
	}
	
	public ColumnBuilder<?,?,?> builder() {
		return builder(0);
	}
	
	public ColumnBuilder<?,?,?> builder(int characteristics) {
		switch(this) {
		case BOOLEAN:
			return BooleanColumn.builder();
		case DATE:
			return DateColumn.builder(characteristics);
		case DATETIME:
			return DateTimeColumn.builder(characteristics);
		case DOUBLE:
			return DoubleColumn.builder(characteristics);
		case FLOAT:
			return FloatColumn.builder(characteristics);
		case INT:
			return IntColumn.builder(characteristics);
		case LONG:
			return LongColumn.builder(characteristics);
		case STRING:
			return StringColumn.builder(characteristics);
		}
		
		throw new IllegalStateException();
	}
	
	public Column<?> nullColumn(int size) {
		return builder().addNulls(size).build();
	}
		
	abstract Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics);
	
	Column<?> fromBuffer(ByteBuffer buffer, int offset, int length, int characteristics, boolean isNullable) {		
		if(!isNullable)
			return fromBuffer(buffer, offset, length, characteristics);
		
		int nonNullLength = buffer.getInt(offset);
		offset += 4;
		
		BufferBitSetWrapper wrapper = readBufferBitSet(slice(buffer, offset, offset+nonNullLength));
	
		int columnStart = offset+nonNullLength;
		Column<?> column = fromBuffer(buffer, columnStart, length - columnStart, characteristics);
		
		switch(this) {
		case BOOLEAN:
			return new NullableBooleanColumn((NonNullBooleanColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);
		case DATE:
			return new NullableDateColumn((NonNullDateColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);
		case DATETIME:
			return new NullableDateTimeColumn((NonNullDateTimeColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);
		case DOUBLE:
			return new NullableDoubleColumn((NonNullDoubleColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);			
		case FLOAT:
			return new NullableFloatColumn((NonNullFloatColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);			
		case INT:
			return new NullableIntColumn((NonNullIntColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);			
		case LONG:
			return new NullableLongColumn((NonNullLongColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);
		case STRING:
			return new NullableStringColumn((NonNullStringColumn)column, wrapper.bbs, wrapper.offset, wrapper.size);
		}
		
		throw new IllegalStateException();
	}
}
