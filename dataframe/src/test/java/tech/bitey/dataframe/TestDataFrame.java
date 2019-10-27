package tech.bitey.dataframe;

import org.junit.jupiter.api.Test;

public class TestDataFrame {

	@Test
	public void basic() {
		
		StringColumn c1 = StringColumn.of("A", "B", "C");
		IntColumn c2 = IntColumn.of(1, 2, 3);
		
		DataFrame df = DataFrameFactory.$.create(new Column<?>[] {c1, c2}, new String[] {"C1", "C2"});
		df.toString();
	}
}
