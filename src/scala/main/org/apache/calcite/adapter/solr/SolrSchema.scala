package org.apache.calcite.adapter.solr

import java.util.Properties

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.immutable.Map

import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient

object SolrSchema {
	def create(rootSchema: SchemaPlus, name: String, props: Properties) = {
		val solrSchema = new SolrSchema(props);
		rootSchema.add("solr", solrSchema);
		solrSchema
	}
}

class SolrSchema(args: Map[String, String]) extends AbstractSchema {

	def this(args: Properties) = this(args.toMap)

	val logger = Logger.getLogger(this.getClass);

	//columns="title string, url string, content_length int"
	val columns: Map[String, SqlTypeName] = args("columns").split("\\s*,\\s*").map { _.split("\\s") }.map { x ⇒ (x(0), toSqlTypeName(x(1))) }.toMap;
	logger.debug(s"defined columns: $columns");

	//columnMapping="title->solr_field_title, url->solr_field_url"
	val definedColumnMapping = args("columnMapping").split("\\s*,\\s*").map { _.split("\\s*->\\s*") }.map { x ⇒ (x(0), x(1)) }.toMap;
	logger.debug(s"defined column mapping: $definedColumnMapping");

	val columnMappingBuffer = collection.mutable.Map[String, String]();
	columns.keys.foreach(x ⇒ if (!definedColumnMapping.contains(x)) { columnMappingBuffer += (x -> x); });

	val columnMapping = definedColumnMapping ++ columnMappingBuffer;

	//options="pageSize:20,solrZkHosts=10.0.71.14:2181,10.0.71.17:2181,10.0.71.38:2181"
	val options = args;
	val solrClient = if (options.containsKey("solrZkHosts")) {
		val solrZkHosts = options("solrZkHosts");
		logger.debug(s"connecting to solr cloud via zookeeper servers: $solrZkHosts");
		val csc = new CloudSolrClient(solrZkHosts);
		csc.setDefaultCollection(options("solrCollection"));
		csc;
	}
	else {
		val solrServerURL = options("solrServerURL");
		logger.debug(s"connecting to solr server: $solrServerURL");
		new HttpSolrClient(solrServerURL);
	}

	override def isMutable = true;
	def toSqlTypeName(typeName: String): SqlTypeName = {
		val sqlType = SqlTypeName.get(typeName.toUpperCase());
		if (sqlType == null)
			throw new SolrSqlException(s"unknown column type:$typeName");

		sqlType;
	}

	override def getTableMap: java.util.Map[String, Table] = Map[String, Table](args.getOrElse("tableName", "docs") -> new SolrTable(solrClient, columns, columnMapping, options));
}