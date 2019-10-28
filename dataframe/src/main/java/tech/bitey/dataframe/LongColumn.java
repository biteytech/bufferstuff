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

public interface LongColumn extends NumericColumn<Long> {

	@Override LongColumn subColumn(int fromIndex, int toIndex);
	
	@Override LongColumn subColumn(Long fromElement, boolean fromInclusive, Long toElement, boolean toInclusive);
	@Override LongColumn subColumn(Long fromElement, Long toElement);
	@Override LongColumn head(Long toElement, boolean inclusive);
	@Override LongColumn head(Long toElement);	
	@Override LongColumn tail(Long fromElement, boolean inclusive);
	@Override LongColumn tail(Long fromElement);

	long getLong(int index);
	
	public static LongColumnBuilder builder(int characteristics) {		
		return new LongColumnBuilder(characteristics);
	}
	
	public static LongColumnBuilder builder() {
		return new LongColumnBuilder(0);
	}
	
	public static LongColumn of(Long... elements) {
		return builder(0).addAll(elements).build();
	}
	
	public static Collector<Long,?,LongColumn> collector(int characteristics) {		
		return Collector.of(
			() -> builder(characteristics),
			LongColumnBuilder::add,
			LongColumnBuilder::append,
			LongColumnBuilder::build
		);
	}
}
