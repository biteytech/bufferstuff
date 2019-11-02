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
	
	DataFrame getDf(String label) {
		return DF_MAP.get(label);
	}
}
