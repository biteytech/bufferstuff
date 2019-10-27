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

public interface FloatColumn extends NumericColumn<Float> {

	@Override FloatColumn subColumn(int fromIndex, int toIndex);
	
	@Override FloatColumn subColumn(Float fromElement, boolean fromInclusive, Float toElement, boolean toInclusive);
	@Override FloatColumn subColumn(Float fromElement, Float toElement);
	@Override FloatColumn head(Float toElement, boolean inclusive);
	@Override FloatColumn head(Float toElement);	
	@Override FloatColumn tail(Float fromElement, boolean inclusive);
	@Override FloatColumn tail(Float fromElement);

	float getFloat(int index);
	
	public static FloatColumnBuilder builder(boolean sortedSet) {		
		return new FloatColumnBuilder(sortedSet);
	}
	
	public static FloatColumn of(Float... elements) {
		return builder(false).addAll(elements).build();
	}
	
	public static Collector<Float,?,FloatColumn> collector(boolean sortedSet) {		
		return Collector.of(
			() -> builder(sortedSet),
			FloatColumnBuilder::add,
			FloatColumnBuilder::append,
			FloatColumnBuilder::build
		);
	}
}
