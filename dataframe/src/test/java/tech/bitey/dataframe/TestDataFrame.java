package tech.bitey.dataframe;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;

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
//		System.out.println(df1.toString());
//		System.out.println();
		
		StringColumn c12 = StringColumn.of("A2", "B2", "C2");
		IntColumn c22 = IntColumn.builder(DISTINCT).addAll(1, 2, 3).build();
		StringColumn c32 = StringColumn.of("one", "two", "three");
		
		DataFrame df2 = DataFrameFactory.$.create(new Column<?>[] {c12, c22, c32}, new String[] {"C1", "C2", "C3"}, "C2");
//		System.out.println(df2.toString());
//		System.out.println();
		
		DataFrame joint = df1.joinSingleIndex(df2, false, "C2");
//		System.out.println(joint.toString());
		
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
}
