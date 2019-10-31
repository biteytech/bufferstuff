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
import static java.util.Spliterator.SORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataFrame {

	@Test
	public void basic() {
		
		StringColumn c1 = StringColumn.of("A", "B", "C");
		IntColumn c2 = IntColumn.of(1, 2, 3);
		
		DataFrame df = DataFrameFactory.$.create(new Column<?>[] {c1, c2}, new String[] {"C1", "C2"});		
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c1}, new String[] {"C1"}).withColumn("C2", c2);
		
		Assertions.assertEquals(df, df2);
	}
	
	@Test
	public void singleColumnJoin() {
				
		StringColumn c11 = StringColumn.of("A", "B", "C");
		IntColumn c21 = IntColumn.builder().addAll(1, 2, 3).build();
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {c11, c21}, new String[] {"C1", "C2"});
		
		StringColumn c12 = StringColumn.of("A2", "B2", "C2");
		IntColumn c22 = IntColumn.builder(DISTINCT).addAll(1, 2, 3).build();
		StringColumn c32 = StringColumn.of("one", "two", "three");
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c12, c22, c32}, new String[] {"C1", "C2", "C3"}, "C2");
		
		DataFrame joint = df1.joinSingleIndex(df2, false, "C2");		
		DataFrame expected = DataFrameFactory.$.create(new Column<?>[] {c11, c22, c12, c32}, new String[] {"C1", "C2", "C1_2", "C3"});		
		Assertions.assertEquals(expected, joint);
	}
	
	@Test
	public void sortedStringIndexOf() {
		
		StringColumn col = StringColumn.builder(SORTED).addAll(new String[] {"a", "b", "b", "b", "b", "b", "b", "b", "c"}).build();
		Assertions.assertEquals(1, col.indexOf("b"));
		Assertions.assertEquals(7, col.lastIndexOf("b"));
	}
	
	@Test
	public void testCopy() {
		StringColumn c1 = StringColumn.of("A", "B", null, "C", "D");
		IntColumn c2 = IntColumn.of(1, 2, null, 3, 4);
		BooleanColumn c3 = BooleanColumn.of(true, false, null, true, false);
		
		DataFrame df = DataFrameFactory.$.create(new Column<?>[] {c1, c2, c3}, new String[] {"C1", "C2", "C3"});
		
		Assertions.assertEquals(df, df.copy());
		
		DataFrame df2 = df.subFrame(1, 4);
		Assertions.assertEquals(df2, df2.copy());
	}
	
	@Test
	public void joinTest() {
		
		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {df1KeyColumn, df1ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		IntColumn df2KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 2, 3, 5).build();
		DoubleColumn df2ValueColumn = DoubleColumn.of(1d, 2d, 3d, 5d);
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {df2KeyColumn, df2ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		DataFrame joint1 = df1.join(df2);
		System.out.println(joint1);
		
		DataFrame joint2 = df1.subFrame(1, df1.size()).join(df2.subFrame(0, df2.size()-1));
		System.out.println(joint2);
	}
	
	@Test
	public void joinSingleIndexTest() {
		
		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {df1KeyColumn, df1ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		IntColumn df2KeyColumn = IntColumn.builder().addAll(1, 2, 3, 5).build();
		DoubleColumn df2ValueColumn = DoubleColumn.of(1d, 2d, 3d, 5d);
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {df2KeyColumn, df2ValueColumn},
				new String[] {"KEY", "VALUE"}, null);
		df2 = df2.append(df2);
		
		DataFrame joint1 = df1.joinSingleIndex(df2, "KEY");
		System.out.println(joint1);
	}
	
	@Test
	public void leftJoinSingleIndexTest() {
		
		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {df1KeyColumn, df1ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		IntColumn df2KeyColumn = IntColumn.builder().addAll(1, 2, 3, 5).build();
		DoubleColumn df2ValueColumn = DoubleColumn.of(1d, 2d, 3d, 5d);
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {df2KeyColumn, df2ValueColumn},
				new String[] {"KEY", "VALUE"}, null);
		df2 = df2.append(df2);
		
		DataFrame joint1 = df1.leftJoinSingleIndex(df2, "KEY");
		System.out.println(joint1);
	}
	
	@Test
	public void joinHashTest() {
		
		IntColumn c1l = IntColumn.of(1, 1, 3, 3, 4, 4, 5, 5);
		DoubleColumn c2l = DoubleColumn.of(1d, 2d, 1d, 2d, 1d, 2d, 1d, 2d);
		StringColumn c3l = StringColumn.of("1,1d","1,2d","3,1d","3,2d","4,1d","4,2d","5,1d","5,2d");
		DataFrame left = DataFrameFactory.$.create(new Column<?>[] {c1l, c2l, c3l},
				new String[] {"KEY1", "KEY2", "VALUE"});
		
		IntColumn c1r = IntColumn.of(1, 3, 4, 3, 6);
		DoubleColumn c2r = DoubleColumn.of(1d, 2d, 1d, 2d, 2d);
		StringColumn c3r = StringColumn.of("1,1d","3,2d","4,1d","3,2d","6,2d");
		DataFrame right = DataFrameFactory.$.create(new Column<?>[] {c1r, c2r, c3r},
				new String[] {"KEY1", "KEY2", "VALUE"});
		
		DataFrame joint = left.joinHash(right, new String[] {"KEY1", "KEY2"}, new String[] {"KEY1", "KEY2"});
		System.out.println(joint);
	}
	
	@Test
	void toMapTest1() throws Exception {
		
		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {df1KeyColumn, df1ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		Map<Integer, String> expected = df1.stream().collect(
				Collectors.toMap(r -> r.getInt(0), r -> r.getString(1)));
		
		Map<Integer, String> actual = df1.toMap("VALUE");
		
		Assertions.assertEquals(expected, actual);
	}
	
	@Test
	void joinSingleIndex() throws Exception {
		
		IntColumn c1 = IntColumn.builder(DISTINCT).addAll(new int[] {0, 1, 3, 5, 6}).build();
		c1 = c1.subColumn(1, c1.size());
		StringColumn s1 = StringColumn.of("one", "three", "five", "six");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {c1, s1},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		IntColumn c2 = IntColumn.of(0, 1, 5, 2, 3, 5, 4, 0);
		c2 = c2.subColumn(1, c2.size()-1);
		StringColumn s2 = StringColumn.of("one", "five", "two", "three", "five", "four");
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c2, s2},
				new String[] {"KEY", "VALUE"});
		
		DataFrame df = df1.joinSingleIndex(df2, "KEY");
		System.out.println(df);
	}
	
	@Test
	void leftJoinSingleIndex() throws Exception {
		
		IntColumn c1 = IntColumn.builder(DISTINCT).addAll(new int[] {0, 1, 3, 5, 6}).build();
		c1 = c1.subColumn(1, c1.size());
		StringColumn s1 = StringColumn.of("one", "three", "five", "six");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {c1, s1},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		IntColumn c2 = IntColumn.of(0, 1, 5, 2, 3, 5, 4, 0);
		c2 = c2.subColumn(1, c2.size()-1);
		StringColumn s2 = StringColumn.of("one", "five", "two", "three", "five", "four");
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c2, s2},
				new String[] {"KEY", "VALUE"});
		
		DataFrame df = df1.leftJoinSingleIndex(df2, "KEY");
		System.out.println(df);
	}
	
	@Test
	void testSaveLoadString() throws Exception {
		
		StringColumn c1 = StringColumn.of("A", "BB", "CCC", "DDDD", "EEEEE", "FFFFFF", "GGGGGGG");
		StringColumn c2 = StringColumn.of(null, "BB", null, "DDDD", null, "FFFFFF", null);
		
		DataFrame df = DataFrameFactory.$.create(new Column<?>[] {c1, c2}, new String[] {"C1", "C2"});
		df = df.subFrame(1, df.size() - 1);
			
		File file = new File(System.getProperty("user.home")+"/Desktop", "df.dat");
		df.writeTo(file);
		
		DataFrame df2 = DataFrameFactory.$.readFrom(file);
		
		assertEquals(df, df2);
	}
}
