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

import java.time.LocalDateTime;
import java.util.stream.Collector;

public interface DateTimeColumn extends Column<LocalDateTime> {

	@Override DateTimeColumn subColumn(int fromIndex, int toIndex);
	
	@Override DateTimeColumn subColumn(LocalDateTime fromElement, boolean fromInclusive, LocalDateTime toElement, boolean toInclusive);
	@Override DateTimeColumn subColumn(LocalDateTime fromElement, LocalDateTime toElement);
	@Override DateTimeColumn head(LocalDateTime toElement, boolean inclusive);
	@Override DateTimeColumn head(LocalDateTime toElement);	
	@Override DateTimeColumn tail(LocalDateTime fromElement, boolean inclusive);
	@Override DateTimeColumn tail(LocalDateTime fromElement);
	
	public static DateTimeColumnBuilder builder(boolean sortedSet) {		
		return new DateTimeColumnBuilder(sortedSet);
	}
	
	public static DateTimeColumn of(LocalDateTime... elements) {
		return builder(false).addAll(elements).build();
	}
	
	public static Collector<LocalDateTime,?,DateTimeColumn> collector(boolean sortedSet) {		
		return Collector.of(
			() -> builder(sortedSet),
			DateTimeColumnBuilder::add,
			DateTimeColumnBuilder::append,
			DateTimeColumnBuilder::build
		);
	}
}
