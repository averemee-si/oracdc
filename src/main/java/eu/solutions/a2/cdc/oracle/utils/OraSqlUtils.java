package eu.solutions.a2.cdc.oracle.utils;

import java.util.Arrays;
import java.util.List;

public class OraSqlUtils {

	private static final String SQL_AND = " and ";

	public static String parseTableSchemaList(final boolean exclude, final boolean logMiner, final List<String> listSchemaObj) {
		String schemaNameField = "L.LOG_OWNER";
		String objNameField = "L.MASTER";
		if (logMiner) {
			schemaNameField = "M.SEG_OWNER";
			objNameField = "M.SEG_NAME";
		}

		final StringBuilder sb = new StringBuilder(512);
		sb.append(SQL_AND);
		sb.append("(");

		for (int i = 0; i < listSchemaObj.size(); i++) {
			final String schemaObj = listSchemaObj.get(i).trim();
			if (schemaObj.contains(".")) {
				final String[] pairSchemaObj = schemaObj.split("\\.");
				if ("%".equals(pairSchemaObj[1]) || "*".equals(pairSchemaObj[1])) {
					// Only schema name present
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("')");
				} else {
					// Process pair... ... ...
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("'");
					sb.append(SQL_AND);
					sb.append(objNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[1]);
					sb.append("')");
				}
			} else {
				// Just plain table name without owner
				sb.append("(");
				sb.append(objNameField);
				sb.append(exclude ? "!='" : "='");
				sb.append(schemaObj);
				sb.append("')");
			}
			if (i < listSchemaObj.size() - 1) {
				if (exclude)
					sb.append(SQL_AND);
				else
					sb.append(" or ");
			}
		}

		sb.append(")");
		return sb.toString();
	}

	public static void main(String[] argv) {
		String[] test = {"GL.GL_CODE_COMBINATIONS", "XLA_AE_LINES", "SCOTT.%"};
		System.out.println(parseTableSchemaList(true, false, Arrays.asList(test)));
	}

}
