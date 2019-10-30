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

import java.util.stream.Collector;

public interface IntColumn extends NumericColumn<Integer> {

	@Override IntColumn subColumn(int fromIndex, int toIndex);
	
	@Override IntColumn subColumn(Integer fromElement, boolean fromInclusive, Integer toElement, boolean toInclusive);
	@Override IntColumn subColumn(Integer fromElement, Integer toElement);
	@Override IntColumn head(Integer toElement, boolean inclusive);
	@Override IntColumn head(Integer toElement);	
	@Override IntColumn tail(Integer fromElement, boolean inclusive);
	@Override IntColumn tail(Integer fromElement);

	int getInt(int index);
	
	public static IntColumnBuilder builder(int characteristics) {		
		return new IntColumnBuilder(characteristics);
	}
	
	public static IntColumnBuilder builder() {
		return new IntColumnBuilder(0);
	}
	
	public static IntColumn of(Integer... elements) {
		return builder(0).addAll(elements).build();
	}
	
	public static Collector<Integer,?,IntColumn> collector(int characteristics) {		
		return Collector.of(
			() -> builder(characteristics),
			IntColumnBuilder::add,
			IntColumnBuilder::append,
			IntColumnBuilder::build
		);
	}
	
	public static Collector<Integer,?,IntColumn> collector() {
		return collector(0);
	}
}
