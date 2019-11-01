package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	DataFrame getDf(String label) {
		return DF_MAP.get(label);
	}
}
