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

public interface DoubleColumn extends NumericColumn<Double> {

	@Override DoubleColumn subColumn(int fromIndex, int toIndex);
	
	@Override DoubleColumn subColumn(Double fromElement, boolean fromInclusive, Double toElement, boolean toInclusive);
	@Override DoubleColumn subColumn(Double fromElement, Double toElement);
	@Override DoubleColumn head(Double toElement, boolean inclusive);
	@Override DoubleColumn head(Double toElement);	
	@Override DoubleColumn tail(Double fromElement, boolean inclusive);
	@Override DoubleColumn tail(Double fromElement);

	double getDouble(int index);
	
	public static DoubleColumnBuilder builder(boolean sortedSet) {		
		return new DoubleColumnBuilder(sortedSet);
	}
	
	public static DoubleColumn of(Double... elements) {
		return builder(false).addAll(elements).build();
	}
	
	public static Collector<Double,?,DoubleColumn> collector(boolean sortedSet) {		
		return Collector.of(
			() -> builder(sortedSet),
			DoubleColumnBuilder::add,
			DoubleColumnBuilder::append,
			DoubleColumnBuilder::build
		);
	}
}
