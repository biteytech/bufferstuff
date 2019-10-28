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

import java.nio.charset.Charset;
import java.util.stream.Collector;

public interface StringColumn extends Column<String> {
	
	static final Charset UTF_8 = Charset.forName("UTF-8");

	@Override StringColumn subColumn(int fromIndex, int toIndex);
	
	@Override StringColumn subColumn(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive);
	@Override StringColumn subColumn(String fromElement, String toElement);
	@Override StringColumn head(String toElement, boolean inclusive);
	@Override StringColumn head(String toElement);	
	@Override StringColumn tail(String fromElement, boolean inclusive);
	@Override StringColumn tail(String fromElement);
	
	public static StringColumnBuilder builder(int characteristics) {		
		return new StringColumnBuilder(characteristics);
	}
	
	public static StringColumnBuilder builder() {
		return new StringColumnBuilder(0);
	}
	
	public static StringColumn of(String... elements) {
		return builder(0).addAll(elements).build();
	}
	
	public static Collector<String,?,StringColumn> collector(int characteristics) {		
		return Collector.of(
			() -> builder(characteristics),
			StringColumnBuilder::add,
			StringColumnBuilder::append,
			StringColumnBuilder::build
		);
	}
}
