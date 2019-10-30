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
	
	public static DoubleColumnBuilder builder(int characteristics) {		
		return new DoubleColumnBuilder(characteristics);
	}
	
	public static DoubleColumnBuilder builder() {
		return new DoubleColumnBuilder(0);
	}
	
	public static DoubleColumn of(Double... elements) {
		return builder(0).addAll(elements).build();
	}
	
	public static Collector<Double,?,DoubleColumn> collector(int characteristics) {		
		return Collector.of(
			() -> builder(characteristics),
			DoubleColumnBuilder::add,
			DoubleColumnBuilder::append,
			DoubleColumnBuilder::build
		);
	}
	
	public static Collector<Double,?,DoubleColumn> collector() {
		return collector(0);
	}
}
