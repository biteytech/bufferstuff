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

import java.time.LocalDate;
import java.util.stream.Collector;

public interface DateColumn extends Column<LocalDate> {

	@Override DateColumn subColumn(int fromIndex, int toIndex);
	
	@Override DateColumn subColumn(LocalDate fromElement, boolean fromInclusive, LocalDate toElement, boolean toInclusive);
	@Override DateColumn subColumn(LocalDate fromElement, LocalDate toElement);
	@Override DateColumn head(LocalDate toElement, boolean inclusive);
	@Override DateColumn head(LocalDate toElement);	
	@Override DateColumn tail(LocalDate fromElement, boolean inclusive);
	@Override DateColumn tail(LocalDate fromElement);
	
	int yyyymmdd(int index);
	
	public static DateColumnBuilder builder(int characteristics) {		
		return new DateColumnBuilder(characteristics);
	}
	
	public static DateColumnBuilder builder() {
		return new DateColumnBuilder(0);
	}
	
	public static DateColumn of(LocalDate... elements) {
		return builder(0).addAll(elements).build();
	}
	
	public static Collector<LocalDate,?,DateColumn> collector(int characteristics) {		
		return Collector.of(
			() -> builder(characteristics),
			DateColumnBuilder::add,
			DateColumnBuilder::append,
			DateColumnBuilder::build
		);
	}
	
	public static Collector<LocalDate,?,DateColumn> collector() {
		return collector(0);
	}
}
