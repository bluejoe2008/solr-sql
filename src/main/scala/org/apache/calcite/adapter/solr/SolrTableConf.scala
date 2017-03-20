package org.apache.calcite.adapter.solr

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.log4j.Logger

/**
 * this object parses value of arguments of a SolrTable
 */
object SolrTableConf {
	val PAGE_SIZE = "pageSize";
	val COULMNS = "columns";
	val COLUMN_MAPPING = "columnMapping";
	val SOLR_ZK_HOSTS = "solrZkHosts";
	val SOLR_SERVER_URL = "solrServerURL";

	val logger = Logger.getLogger(this.getClass);

	/**
	 * asserts a given argument is assigned a value
	 */
	def argumentsRequired(args: Map[String, String], names: String*): Unit =
		{
			for (name ← names) {
				if (names.contains(name))
					return ;
			}
			if (names.length == 1)
				throw new SolrSqlException(s"argument should have value: ${names(0)}");
			else
				throw new SolrSqlException(s"one of arguments should have value: ${names}");
		}

	//parse argument safely
	def parseSafeArgument[T](args: Map[String, String], columnName: String, expected: String, defaultValue: String)(parse: (String) ⇒ T): T = {
		val value = args.getOrElse(columnName, {
			logger.debug(s"no value assigned to '$columnName', use default value: $defaultValue");
			defaultValue
		});
		try {
			parse(value);
		}
		catch {
			case e: Throwable ⇒
				throw new SolrSqlException(s"wrong format of column '$columnName': $value, expected: $expected", e);
		}
	}

	private def toSqlTypeName(typeName: String): SqlTypeName = {
		val sqlType = SqlTypeName.get(typeName.toUpperCase());
		if (sqlType == null)
			throw new SolrSqlException(s"unknown column type:$typeName");

		sqlType;
	}

	def parseString(args: Map[String, String], columnName: String, defaultValue: String = ""): String =
		parseSafeArgument(args, columnName, "a string, e.g. hello", defaultValue) {
			_.toString();
		}

	def parseInt(args: Map[String, String], columnName: String, defaultValue: String = ""): Int =
		parseSafeArgument(args, columnName, "a integer number, e.g. 100", defaultValue) {
			Integer.parseInt(_);
		}

	def parseMap(args: Map[String, String], columnName: String, defaultValue: String = ""): Map[String, String] =
		parseSafeArgument(args, columnName, "comma seperated column mappings, each column mapping is describled in format 'columnName->field_name_in_solr_document', e.g. 'name->name_s, age->age_i'", defaultValue) {
			_.split("\\s*,\\s*").map { _.split("\\s*->\\s*") }.map { x ⇒ (x(0), x(1)) }.toMap;
		}

	def parseColumns(args: Map[String, String], columnName: String, defaultValue: String = ""): Map[String, SqlTypeName] =
		parseSafeArgument(args, columnName, "comma seperated column definitions, each column is describled in format 'column_name column_type_name', e.g. 'id integer, name char, age integer'", defaultValue) {
			_.split("\\s*,\\s*").map { _.split("\\s") }.map { x ⇒ (x(0), toSqlTypeName(x(1))) }.toMap;
		}
}