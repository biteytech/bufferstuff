package tech.bitey.dataframe;

import static java.util.Spliterator.DISTINCT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataFrame {

	private final Map<String, DataFrame> DF_MAP = new HashMap<>();
	
	@SuppressWarnings({ "rawtypes" })
	TestDataFrame() {
		
		Map<String, List<Column<?>>> columnTestMap = new HashMap<>();
		new TestIntColumn().samples().forEach(s -> {
			List<Column<?>> list;
			columnTestMap.put(s.toString(), list = new ArrayList<>());
			list.add(s.column());
		});
		
		TestColumn[] columnTest = new TestColumn[] {new TestLongColumn(), new TestFloatColumn(), new TestDoubleColumn(), new TestStringColumn(), new TestBooleanColumn()};
		
		for(TestColumn<?> tests : columnTest) {
			tests.samples().forEach(s -> {
				List<Column<?>> list = columnTestMap.get(s.toString());
				if(list != null && list.get(0).size() == s.size())
					list.add(s.column());
			});
		}
		
		for(Map.Entry<String, List<Column<?>>> e : columnTestMap.entrySet()) {
						
			Column<?>[] columns = e.getValue().toArray(new Column<?>[0]);			
			String[] columnNames = new String[columns.length];
			for(int i = 0; i < columnNames.length; i++)
				columnNames[i] = "C"+i;
			
			DataFrame df = DataFrameFactory.$.create(columns, columnNames);
			DF_MAP.put(e.getKey(), df);
		}
	}
	
	@Test
	public void testCopy() {
		for(Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {
			
			DataFrame df = e.getValue();
			DataFrame copy = df.copy();
			
			Assertions.assertEquals(df, copy, e.getKey()+", copy");
		}
	}
	
	@Test
	public void testAppend() {
		for(Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {
			
			DataFrame df = e.getValue();
			if(df.size() < 2)
				continue;
			
			int half = df.size()/2;
			DataFrame head = df.subFrame(0, half);
			DataFrame tail = df.subFrame(half, df.size());
			
			DataFrame joint = head.append(tail);
			
			Assertions.assertEquals(df, joint, e.getKey()+", append");
		}
	}
	
	@Test
	public void testHashJoin() {
		
		for(Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {
			
			DataFrame expected = e.getValue();
			
			if(expected.stream().collect(Collectors.toSet()).size() < expected.size())
				continue;
			
			String[] columnNames = expected.columnNames().stream().toArray(String[]::new);
			DataFrame actual = expected.joinHash(expected, columnNames, columnNames);
			
			Assertions.assertEquals(expected, actual, e.getKey()+", hashJoin");
		}
	}
	
	@Test
	public void testSingleColumnJoin() {
				
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
	public void testHashJoin2() {
		
		StringColumn c11 = StringColumn.of("A", "A", "B", "B", "C", "C");
		IntColumn c21 = IntColumn.of(1, 2, 3, 1, 2, 3);
		StringColumn c31 = StringColumn.of("foo", "foo", "foo", "foo", "foo", "foo");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {c11, c21, c31}, new String[] {"KEY1", "KEY2", "FOO"});
		
		StringColumn c12 = StringColumn.of("B", "B", "C", "C", "D", "D");
		IntColumn c22 = IntColumn.of(2, 3, 4, 2, 3, 4);
		StringColumn c32 = StringColumn.of("bar", "bar", "bar", "bar", "bar", "bar");
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c12, c22, c32}, new String[] {"KEY1", "KEY2", "BAR"});
		
		DataFrame actual = df1.joinHash(df2, new String[] {"KEY1","KEY2"}, new String[] {"KEY1","KEY2"});
		
		StringColumn e1 = StringColumn.of("B", "C");
		IntColumn e2 = IntColumn.of(3, 2);
		StringColumn e3 = StringColumn.of("foo", "foo");
		StringColumn e4 = StringColumn.of("bar", "bar");
		DataFrame expected = DataFrameFactory.$.create(new Column<?>[] {e1, e2, e3, e4}, new String[] {"KEY1", "KEY2", "FOO", "BAR"});
		
		Assertions.assertEquals(expected, actual);
	}
	
	@Test
	void testToMap() throws Exception {
		
		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.$.create(new Column<?>[] {df1KeyColumn, df1ValueColumn},
				new String[] {"KEY", "VALUE"}, "KEY");
		
		Map<Integer, String> expected = df1.stream().collect(
				Collectors.toMap(r -> r.getInt(0), r -> r.getString(1)));
		
		Map<Integer, String> actual = df1.toMap("VALUE");
		
		Assertions.assertEquals(expected, actual);
	}
	
	DataFrame getDf(String label) {
		return DF_MAP.get(label);
	}
}
