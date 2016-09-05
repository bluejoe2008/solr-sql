package org.apache.calcite.adapter.solr

import org.apache.calcite.schema.SchemaFactory
import org.apache.solr.client.solrj.SolrClient
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.calcite.sql.`type`.SqlTypeName
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.calcite.schema.TableFactory
import org.apache.solr.client.solrj.impl.HttpSolrClient
import scala.collection.JavaConversions

trait SolrClientFactory {
	def getClient(): SolrClient;
}

class SolrTableFactory extends TableFactory[SolrTable] {
	val logger = Logger.getLogger(this.getClass);

	private def toSqlTypeName(typeName: String): SqlTypeName = {
		val sqlType = SqlTypeName.get(typeName.toUpperCase());
		if (sqlType == null)
			throw new SolrSqlException(s"unknown column type:$typeName");

		sqlType;
	}

	private def argumentsRequired(args: Map[String, String], names: String*): Unit =
		{
			for (name ← names) {
				if (names.contains(name))
					return ;
			}

			throw new SolrSqlException(s"arguments should be have value: $names");
		}

	//parse argument safely
	private def parseSafeArgument[T](args: Map[String, String], columnName: String, defaultValue: String = "")(parse: (String) ⇒ T): T = {
		val value = args.getOrElse(columnName, defaultValue);
		try {
			parse(value);
		}
		catch {
			case e: Throwable ⇒
				throw new SolrSqlException(s"wrong format of column '$columnName': $value", e);
		}
	}

	override def create(parentSchema: SchemaPlus, name: String,
		operands: java.util.Map[String, Object], rowTypw: RelDataType): SolrTable = {

		val args = JavaConversions.mapAsScalaMap(operands).toMap.map(x ⇒ (x._1, x._2.toString()));
		//columns="title string, url string, content_length int"
		argumentsRequired(args, "columns");

		val columns: Map[String, SqlTypeName] = parseSafeArgument(args, "columns") {
			_.split("\\s*,\\s*").map { _.split("\\s") }.map { x ⇒ (x(0), toSqlTypeName(x(1))) }.toMap;
		}

		logger.debug(s"defined columns: $columns");

		//columnMapping="title->solr_field_title, url->solr_field_url"
		val definedColumnMapping = parseSafeArgument(args, "columnMapping") {
			_.split("\\s*,\\s*").map { _.split("\\s*->\\s*") }.map { x ⇒ (x(0), x(1)) }.toMap;
		}

		logger.debug(s"defined column mapping: $definedColumnMapping");

		val columnMappingBuffer = collection.mutable.Map[String, String]();
		columns.keys.foreach(x ⇒ if (!definedColumnMapping.contains(x)) { columnMappingBuffer += (x -> x); });

		val columnMapping = definedColumnMapping ++ columnMappingBuffer;

		//options="pageSize:20,solrZkHosts=10.0.71.14:2181,10.0.71.17:2181,10.0.71.38:2181"
		val options = args;
		//singleton
		val solrClientFactory = new SolrClientFactory {
			val clients = ArrayBuffer[SolrClient]();
			override def getClient = {
				if (clients.isEmpty) {
					if (options.keySet.contains("solrZkHosts")) {
						val solrZkHosts = options("solrZkHosts");
						logger.debug(s"connecting to solr cloud via zookeeper servers: $solrZkHosts");
						val csc = new CloudSolrClient(solrZkHosts);
						csc.setDefaultCollection(options("solrCollection"));
						clients += csc;
					}
					else {
						argumentsRequired(args, "solrZkHosts", "solrServerURL");

						val solrServerURL = options("solrServerURL");
						logger.debug(s"connecting to solr server: $solrServerURL");
						clients += new HttpSolrClient(solrServerURL);
					}
				}

				clients(0);
			}
		}

		new SolrTable(solrClientFactory, columns, columnMapping, options);
	}
}