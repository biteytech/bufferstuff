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

import static java.util.Spliterator.DISTINCT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumn {
	
	@Test
	public void StringColumnTest() {
		StringColumn column = StringColumn.builder(DISTINCT).addAll(new String[] {"a", "b", "c", "d"}).build();
		List<String> list = Arrays.asList("a", "b", "c", "d");
				
		assertEquals(column, list.stream().collect(StringColumn.collector()));
		
		StringColumn sub = column.subColumn("b", "c");
		
		assertEquals(column, list);
		assertEquals(sub, list.subList(1, 3));
		assertEquals(column.head("c"), list.subList(0, 3));
		Assertions.assertEquals(column.tail("b"), list.subList(1, 4));
		
		assertNull(sub.lower("b"));
		assertEquals(sub.lower("b_"), "b");
		assertEquals(sub.lower("c"), "b");
		assertEquals(sub.lower("c_"), "c");
		
		assertNull(sub.floor("a"));
		assertEquals(sub.floor("b"), "b");
		assertEquals(sub.floor("b_"), "b");
		assertEquals(sub.floor("c"), "c");
		assertEquals(sub.floor("c_"), "c");
		
		assertEquals(sub.higher("a"), "b");
		assertEquals(sub.higher("b"), "c");
		assertEquals(sub.higher("b_"), "c");
		assertNull(sub.higher("c"));
		assertNull(sub.higher("c_"));
		
		assertEquals(sub.ceiling("a"), "b");
		assertEquals(sub.ceiling("b"), "b");
		assertEquals(sub.ceiling("b_"), "c");
		assertEquals(sub.ceiling("c"), "c");
		assertNull(sub.ceiling("c_"));
		
		StringColumn c1 = StringColumn.of("1", "2", "1", "2");
		StringColumn c2 = StringColumn.of("1", "2", "1", "2");
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		StringColumn sub1 = c1.subColumn(0, 2);
		StringColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
	}
	
	@Test
	public void BooleanColumnTest() {
		List<Boolean> list = new ArrayList<>(Arrays.asList(true, false, false));
		list.add(null);
		
		BooleanColumn column = BooleanColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		BooleanColumn c1 = BooleanColumn.of(true, false, true, false);
		BooleanColumn c2 = BooleanColumn.of(true, false, true, false);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		BooleanColumn sub1 = c1.subColumn(0, 2);
		BooleanColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(BooleanColumn.collector()));
	}
	
	@Test
	public void DoubleColumnTest() {
		List<Double> list = new ArrayList<>(Arrays.asList(1d, 2d, 3d));
		list.add(null);
		
		DoubleColumn column = DoubleColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		DoubleColumn c1 = DoubleColumn.of(1d, 2d, 1d, 2d);
		DoubleColumn c2 = DoubleColumn.of(1d, 2d, 1d, 2d);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		DoubleColumn sub1 = c1.subColumn(0, 2);
		DoubleColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(DoubleColumn.collector()));
	}
	
	@Test
	public void FloatColumnTest() {
		List<Float> list = new ArrayList<>(Arrays.asList(1f, 2f, 3f));
		list.add(null);
		
		FloatColumn column = FloatColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		FloatColumn c1 = FloatColumn.of(1f, 2f, 1f, 2f);
		FloatColumn c2 = FloatColumn.of(1f, 2f, 1f, 2f);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		FloatColumn sub1 = c1.subColumn(0, 2);
		FloatColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(FloatColumn.collector()));
	}
	
	@Test
	public void DateColumnTest() {
		List<LocalDate> list = new ArrayList<>(Arrays.asList(LocalDate.now(), LocalDate.now().plusDays(1), LocalDate.now().plusDays(2)));
		list.add(null);
		
		DateColumn column = DateColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		LocalDate d1 = LocalDate.now();
		LocalDate d2 = LocalDate.now().plusDays(1);
		DateColumn c1 = DateColumn.of(d1, d2, d1, d2);
		DateColumn c2 = DateColumn.of(d1, d2, d1, d2);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		DateColumn sub1 = c1.subColumn(0, 2);
		DateColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(DateColumn.collector()));
	}
	
	@Test
	public void DateTimeColumnTest() {
		List<LocalDateTime> list = new ArrayList<>(Arrays.asList(LocalDateTime.now(), LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(2)));
		list.add(null);
		
		DateTimeColumn column = DateTimeColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		LocalDateTime d1 = LocalDateTime.now();
		LocalDateTime d2 = LocalDateTime.now().plusDays(1);
		DateTimeColumn c1 = DateTimeColumn.of(d1, d2, d1, d2);
		DateTimeColumn c2 = DateTimeColumn.of(d1, d2, d1, d2);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		DateTimeColumn sub1 = c1.subColumn(0, 2);
		DateTimeColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(DateTimeColumn.collector()));
	}
	
	@Test
	public void IntColumnTest() {
		List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
		list.add(null);
		
		IntColumn column = IntColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		IntColumn c1 = IntColumn.of(1, 2, 1, 2);
		IntColumn c2 = IntColumn.of(1, 2, 1, 2);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		IntColumn sub1 = c1.subColumn(0, 2);
		IntColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(IntColumn.collector()));
	}
	
	@Test
	public void LongColumnTest() {
		List<Long> list = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
		list.add(null);
		
		LongColumn column = LongColumn.builder().addAll(list).build();
		assertEquals(column.subColumn(2, 4), list.subList(2, 4));
		
		LongColumn c1 = LongColumn.of(1L, 2L, 1L, 2L);
		LongColumn c2 = LongColumn.of(1L, 2L, 1L, 2L);
		assertEquals(c1.hashCode(), c2.hashCode());
		assertEquals(c1, c2);
		
		LongColumn sub1 = c1.subColumn(0, 2);
		LongColumn sub2 = c2.subColumn(2, 4);
		assertEquals(sub1.hashCode(), sub2.hashCode());
		assertEquals(sub1, sub2);
		
		assertEquals(column, list.stream().collect(LongColumn.collector()));
	}
}
