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

import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

public enum ColumnType {

	BOOLEAN("B") {
	},
	DATE("DA") {
	},
	DATETIME("DT") {
	},
	DOUBLE("D") {
	},
	FLOAT("F") {
	},
	INT("I") {
	},
	LONG("L") {
	},
	STRING("S") {
	},
	;
	
	private final String code;
	
	private ColumnType(String code) {
		
		byte[] codeBytes = code.getBytes();
		checkState(codeBytes.length >= 1 && codeBytes.length <= 2,
				"code must be one or two (ascii) characters");
		
		this.code = code;
	}
	
	String getCode() {
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
}
