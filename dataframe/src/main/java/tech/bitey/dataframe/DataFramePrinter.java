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

import static tech.bitey.dataframe.guava.DfStrings.isNullOrEmpty;
import static tech.bitey.dataframe.guava.DfStrings.repeat;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.stream.IntStream;

/**
 * A class that can pretty print a DataFrame to text for visualization in a
 * console
 * <p>
 * Based off of
 * https://github.com/jtablesaw/tablesaw/blob/master/core/src/main/java/tech/tablesaw/io/string/DataFramePrinter.java,
 * which is in turn based off of:
 * <p>
 * Based off of
 * https://github.com/zavtech/morpheus-core/blob/master/src/main/java/com/zavtech/morpheus/reference/XDataFramePrinter.java
 * under Apache 2 license
 */
public class DataFramePrinter {

	private static final String TOO_SHORT_COLUMN_MARKER = "?";
	
	private static final int PADDING = 1;

	private final int maxRows;
//	private final OutputStream stream;

	/**
	 * Constructor
	 *
	 * @param maxRows the max rows to print
	 */
	public DataFramePrinter(int maxRows) {
		this.maxRows = maxRows;
//		this.stream = stream;
	}

	/**
	 * Returns the column widths required to print the header and data
	 *
	 * @param headers the headers to print
	 * @param data    the data items to print
	 * @return the required column widths
	 */
	private static int[] getWidths(String[] headers, String[][] data) {
		final int[] widths = new int[headers.length];
		for (int j = 0; j < headers.length; j++) {
			final String header = headers[j];
			widths[j] = Math.max(widths[j], header != null ? header.length() : 0);
		}
		for (String[] rowValues : data) {
			for (int j = 0; j < rowValues.length; j++) {
				final String value = rowValues[j];
				widths[j] = Math.max(widths[j], value != null ? value.length() : 0);
			}
		}
		return widths;
	}

	/**
	 * Returns the header template given the widths specified
	 *
	 * @param widths the token widths
	 * @return the line format template
	 */
	private static String getHeaderTemplate(int[] widths, String[] headers) {
		return IntStream.range(0, widths.length).mapToObj(i -> {
			final int width = widths[i];
			final int length = headers[i].length();
			final int leading = (width - length) / 2;
			final int trailing = width - (length + leading);
			final StringBuilder text = new StringBuilder();
			whitespace(text, leading + PADDING - 1);
			text.append("%").append(i + 1).append("$s");
			whitespace(text, trailing + PADDING);			
			text.append("|");
			return text.toString();
		}).reduce((left, right) -> left + " " + right).orElse("");
	}

	/**
	 * Returns the data template given the widths specified
	 *
	 * @param widths the token widths
	 * @return the line format template
	 */
	private static String getDataTemplate(int[] widths) {
		String LEFT_PADDING = repeat(" ", PADDING-1);
		String RIGHT_PADDING = repeat(" ", PADDING);
		return IntStream.range(0, widths.length).mapToObj(i -> LEFT_PADDING + "%" + (i + 1) + "$" + widths[i] + "s" + RIGHT_PADDING + "|")
				.reduce((left, right) -> left + " " + right).orElse("");
	}

	/**
	 * Returns a whitespace string of the length specified
	 *
	 * @param length the length for whitespace
	 */
	private static void whitespace(StringBuilder text, int length) {
		IntStream.range(0, length).forEach(i -> text.append(" "));
	}

	public String print(DataFrame frame) {
//		try {
		final String[] headers = getHeaderTokens(frame);
		final String[][] data = getDataTokens(frame);
		final int[] widths = getWidths(headers, data);
		final String dataTemplate = getDataTemplate(widths);
		final String headerTemplate = getHeaderTemplate(widths, headers);
		final int totalWidth = IntStream.of(widths).map(w -> w + 2*PADDING + 1).sum() - 1;
		final int totalHeight = data.length + 1;
		int capacity = totalWidth * totalHeight;
		if (capacity < 0) {
			capacity = 0;
		}
		final StringBuilder text = new StringBuilder(capacity);
//			if (frame.name() != null) {
//				text.append(tableName(frame, totalWidth)).append(System.lineSeparator());
//			}
		final String headerLine = String.format(headerTemplate, (Object[]) headers);
		text.append(headerLine).append(System.lineSeparator());
		for (int j = 0; j < totalWidth; j++) {
			text.append("-");
		}
		for (String[] row : data) {
			final String dataLine = String.format(dataTemplate, (Object[]) row);
			text.append(System.lineSeparator());
			text.append(dataLine);
		}
//			final byte[] bytes = text.toString().getBytes();
//			this.stream.write(bytes);
//			this.stream.flush();
		return text.toString();
//		} catch (IOException ex) {
//			throw new IllegalStateException("Failed to print DataFrame", ex);
//		}
	}

//	private String tableName(Relation frame, int width) {
//		if (frame.name().length() > width) {
//			return frame.name();
//		}
//		int diff = width - frame.name().length();
//		String result = StringUtils.repeat(" ", diff / 2) + frame.name();
//		return result + StringUtils.repeat(" ", width - result.length());
//	}

	/**
	 * Returns the header string tokens for the frame
	 *
	 * @param frame the frame to create header tokens
	 * @return the header tokens
	 */
	private String[] getHeaderTokens(DataFrame frame) {
		final int colCount = frame.columnCount();
		final String[] header = new String[colCount];
		Integer keyColumnIndex = frame.keyColumnIndex();
		IntStream.range(0, colCount).forEach(colIndex -> {
			header[colIndex] = frame.columnName(colIndex)
					+" <"+frame.columnType(colIndex).getCode();	
			if(keyColumnIndex != null && keyColumnIndex == colIndex) {
				header[colIndex] += "*";
			}
			header[colIndex] += ">";
		});
		return header;
	}

	private static final MathContext ROUNDING_CONTEXT = new MathContext(8); 
	
	private String getDataToken(Column<?> col, int i) {
//		return col.size() > i ? col.getString(i) : TOO_SHORT_COLUMN_MARKER;
		if (col.size() > i) {
			Object o = col.get(i);
			if(o == null)
				return "(null)";
			else {
				switch(col.getType()) {
				case FLOAT: case DOUBLE:
					Number n = (Number)o;
					return BigDecimal.valueOf(n.doubleValue())
							.round(ROUNDING_CONTEXT).toPlainString();
				case STRING:
					String s = (String)o;
					s = s.replaceAll("\r|\n", "");
					s = s.replaceAll("\t", "  ");
					return StringUtils.abbreviate(s, 100);
				default:
					return o.toString();
				}
			}
		} else
			return TOO_SHORT_COLUMN_MARKER;
	}

	/**
	 * Returns the 2-D array of data tokens from the frame specified
	 *
	 * @param frame the DataFrame from which to create 2D array of formatted tokens
	 * @return the array of data tokens
	 */
	private String[][] getDataTokens(DataFrame frame) {
		if (frame.size() == 0)
			return new String[0][0];
		final int rowCount = Math.min(maxRows, frame.size());
		final boolean truncated = frame.size() > maxRows;
		final int colCount = frame.columnCount();
		final String[][] data;
		if (truncated) {
			data = new String[rowCount + 1][colCount];
			int i;
			for (i = 0; i < Math.ceil((double) rowCount / 2); i++) {
				for (int j = 0; j < colCount; j++) {
					Column<?> col = frame.column(j);
					data[i][j] = getDataToken(col, i);
				}
			}
			for (int j = 0; j < colCount; j++) {
				data[i][j] = "...";
			}
			for (++i; i <= rowCount; i++) {
				for (int j = 0; j < colCount; j++) {
					Column<?> col = frame.column(j);
					data[i][j] = getDataToken(col, frame.size() - maxRows + i - 1);
				}
			}
		} else {
			data = new String[rowCount][colCount];
			for (int i = 0; i < rowCount; i++) {
				for (int j = 0; j < colCount; j++) {
					Column<?> col = frame.column(j);
					String value = getDataToken(col, i);
					data[i][j] = value == null ? "" : value;
				}
			}
		}
		return data;
	}
	
	
	
	
	
	
	
	// from Apache commons lang3
	private static class StringUtils {

		/**
	     * <p>Abbreviates a String using ellipses. This will turn
	     * "Now is the time for all good men" into "Now is the time for..."</p>
	     *
	     * <p>Specifically:</p>
	     * <ul>
	     *   <li>If the number of characters in {@code str} is less than or equal to
	     *       {@code maxWidth}, return {@code str}.</li>
	     *   <li>Else abbreviate it to {@code (substring(str, 0, max-3) + "...")}.</li>
	     *   <li>If {@code maxWidth} is less than {@code 4}, throw an
	     *       {@code IllegalArgumentException}.</li>
	     *   <li>In no case will it return a String of length greater than
	     *       {@code maxWidth}.</li>
	     * </ul>
	     *
	     * <pre>
	     * StringUtils.abbreviate(null, *)      = null
	     * StringUtils.abbreviate("", 4)        = ""
	     * StringUtils.abbreviate("abcdefg", 6) = "abc..."
	     * StringUtils.abbreviate("abcdefg", 7) = "abcdefg"
	     * StringUtils.abbreviate("abcdefg", 8) = "abcdefg"
	     * StringUtils.abbreviate("abcdefg", 4) = "a..."
	     * StringUtils.abbreviate("abcdefg", 3) = IllegalArgumentException
	     * </pre>
	     *
	     * @param str  the String to check, may be null
	     * @param maxWidth  maximum length of result String, must be at least 4
	     * @return abbreviated String, {@code null} if null String input
	     * @throws IllegalArgumentException if the width is too small
	     * @since 2.0
	     */
	    public static String abbreviate(final String str, final int maxWidth) {
	        final String defaultAbbrevMarker = "...";
	        return abbreviate(str, defaultAbbrevMarker, 0, maxWidth);
	    }

	    /**
	     * <p>Abbreviates a String using another given String as replacement marker. This will turn
	     * "Now is the time for all good men" into "Now is the time for..." if "..." was defined
	     * as the replacement marker.</p>
	     *
	     * <p>Specifically:</p>
	     * <ul>
	     *   <li>If the number of characters in {@code str} is less than or equal to
	     *       {@code maxWidth}, return {@code str}.</li>
	     *   <li>Else abbreviate it to {@code (substring(str, 0, max-abbrevMarker.length) + abbrevMarker)}.</li>
	     *   <li>If {@code maxWidth} is less than {@code abbrevMarker.length + 1}, throw an
	     *       {@code IllegalArgumentException}.</li>
	     *   <li>In no case will it return a String of length greater than
	     *       {@code maxWidth}.</li>
	     * </ul>
	     *
	     * <pre>
	     * StringUtils.abbreviate(null, "...", *)      = null
	     * StringUtils.abbreviate("abcdefg", null, *)  = "abcdefg"
	     * StringUtils.abbreviate("", "...", 4)        = ""
	     * StringUtils.abbreviate("abcdefg", ".", 5)   = "abcd."
	     * StringUtils.abbreviate("abcdefg", ".", 7)   = "abcdefg"
	     * StringUtils.abbreviate("abcdefg", ".", 8)   = "abcdefg"
	     * StringUtils.abbreviate("abcdefg", "..", 4)  = "ab.."
	     * StringUtils.abbreviate("abcdefg", "..", 3)  = "a.."
	     * StringUtils.abbreviate("abcdefg", "..", 2)  = IllegalArgumentException
	     * StringUtils.abbreviate("abcdefg", "...", 3) = IllegalArgumentException
	     * </pre>
	     *
	     * @param str  the String to check, may be null
	     * @param abbrevMarker  the String used as replacement marker
	     * @param maxWidth  maximum length of result String, must be at least {@code abbrevMarker.length + 1}
	     * @return abbreviated String, {@code null} if null String input
	     * @throws IllegalArgumentException if the width is too small
	     * @since 3.6
	     */
	    public static String abbreviate(final String str, final String abbrevMarker, final int maxWidth) {
	        return abbreviate(str, abbrevMarker, 0, maxWidth);
	    }

	    /**
	     * <p>Abbreviates a String using a given replacement marker. This will turn
	     * "Now is the time for all good men" into "...is the time for..." if "..." was defined
	     * as the replacement marker.</p>
	     *
	     * <p>Works like {@code abbreviate(String, String, int)}, but allows you to specify
	     * a "left edge" offset.  Note that this left edge is not necessarily going to
	     * be the leftmost character in the result, or the first character following the
	     * replacement marker, but it will appear somewhere in the result.
	     *
	     * <p>In no case will it return a String of length greater than {@code maxWidth}.</p>
	     *
	     * <pre>
	     * StringUtils.abbreviate(null, null, *, *)                 = null
	     * StringUtils.abbreviate("abcdefghijklmno", null, *, *)    = "abcdefghijklmno"
	     * StringUtils.abbreviate("", "...", 0, 4)                  = ""
	     * StringUtils.abbreviate("abcdefghijklmno", "---", -1, 10) = "abcdefg---"
	     * StringUtils.abbreviate("abcdefghijklmno", ",", 0, 10)    = "abcdefghi,"
	     * StringUtils.abbreviate("abcdefghijklmno", ",", 1, 10)    = "abcdefghi,"
	     * StringUtils.abbreviate("abcdefghijklmno", ",", 2, 10)    = "abcdefghi,"
	     * StringUtils.abbreviate("abcdefghijklmno", "::", 4, 10)   = "::efghij::"
	     * StringUtils.abbreviate("abcdefghijklmno", "...", 6, 10)  = "...ghij..."
	     * StringUtils.abbreviate("abcdefghijklmno", "*", 9, 10)    = "*ghijklmno"
	     * StringUtils.abbreviate("abcdefghijklmno", "'", 10, 10)   = "'ghijklmno"
	     * StringUtils.abbreviate("abcdefghijklmno", "!", 12, 10)   = "!ghijklmno"
	     * StringUtils.abbreviate("abcdefghij", "abra", 0, 4)       = IllegalArgumentException
	     * StringUtils.abbreviate("abcdefghij", "...", 5, 6)        = IllegalArgumentException
	     * </pre>
	     *
	     * @param str  the String to check, may be null
	     * @param abbrevMarker  the String used as replacement marker
	     * @param offset  left edge of source String
	     * @param maxWidth  maximum length of result String, must be at least 4
	     * @return abbreviated String, {@code null} if null String input
	     * @throws IllegalArgumentException if the width is too small
	     * @since 3.6
	     */
	    public static String abbreviate(final String str, final String abbrevMarker, int offset, final int maxWidth) {
	        if (isNullOrEmpty(str) || isNullOrEmpty(abbrevMarker)) {
	            return str;
	        }

	        final int abbrevMarkerLength = abbrevMarker.length();
	        final int minAbbrevWidth = abbrevMarkerLength + 1;
	        final int minAbbrevWidthOffset = abbrevMarkerLength + abbrevMarkerLength + 1;

	        if (maxWidth < minAbbrevWidth) {
	            throw new IllegalArgumentException(String.format("Minimum abbreviation width is %d", minAbbrevWidth));
	        }
	        if (str.length() <= maxWidth) {
	            return str;
	        }
	        if (offset > str.length()) {
	            offset = str.length();
	        }
	        if (str.length() - offset < maxWidth - abbrevMarkerLength) {
	            offset = str.length() - (maxWidth - abbrevMarkerLength);
	        }
	        if (offset <= abbrevMarkerLength+1) {
	            return str.substring(0, maxWidth - abbrevMarkerLength) + abbrevMarker;
	        }
	        if (maxWidth < minAbbrevWidthOffset) {
	            throw new IllegalArgumentException(String.format("Minimum abbreviation width with offset is %d", minAbbrevWidthOffset));
	        }
	        if (offset + maxWidth - abbrevMarkerLength < str.length()) {
	            return abbrevMarker + abbreviate(str.substring(offset), abbrevMarker, maxWidth - abbrevMarkerLength);
	        }
	        return abbrevMarker + str.substring(str.length() - (maxWidth - abbrevMarkerLength));
	    }

	}
}