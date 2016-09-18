package org.apache.calcite.adapter.solr

import scala.annotation.migration
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.TableFactory
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient

/**
 * SolrClientFactory creates a client to solr cloud
 */
trait SolrClientFactory {
	def getClient(): SolrClient;
}

class SolrTableFactory extends TableFactory[SolrTable] {
	val logger = Logger.getLogger(this.getClass);

	override def create(parentSchema: SchemaPlus, name: String,
		operands: java.util.Map[String, Object], rowTypw: RelDataType): SolrTable = {

		val args = JavaConversions.mapAsScalaMap(operands).toMap.map(x ⇒ (x._1, x._2.toString()));
		//columns="title string, url string, content_length int"
		SolrTableConf.argumentsRequired(args, SolrTableConf.COULMNS);

		val columns: Map[String, SqlTypeName] = SolrTableConf.parseColumns(args, SolrTableConf.COULMNS);
		logger.debug(s"defined columns: $columns");

		//columnMapping="title->solr_field_title, url->solr_field_url"
		val definedColumnMapping = SolrTableConf.parseMap(args, SolrTableConf.COLUMN_MAPPING);
		logger.debug(s"defined column mapping: $definedColumnMapping");

		val filledColumnMapping = columns.map(x ⇒ (x._1, definedColumnMapping.getOrElse(x._1, x._1)));

		//options="pageSize:20,solrZkHosts=10.0.71.14:2181,10.0.71.17:2181,10.0.71.38:2181"
		val options = args;

		//a singleton of solr client
		val solrClientFactory = new SolrClientFactory {
			val clients = ArrayBuffer[SolrClient]();
			override def getClient = {
				if (clients.isEmpty) {
					if (options.keySet.contains(SolrTableConf.SOLR_ZK_HOSTS)) {
						val solrZkHosts = options(SolrTableConf.SOLR_ZK_HOSTS);
						logger.debug(s"connecting to solr cloud via zookeeper servers: $solrZkHosts");
						val csc = new CloudSolrClient(solrZkHosts);
						csc.setDefaultCollection(options("solrCollection"));
						clients += csc;
					}
					else {
						SolrTableConf.argumentsRequired(args, SolrTableConf.SOLR_ZK_HOSTS, SolrTableConf.SOLR_SERVER_URL);

						val solrServerURL = options(SolrTableConf.SOLR_SERVER_URL);
						logger.debug(s"connecting to solr server: $solrServerURL");
						clients += new HttpSolrClient(solrServerURL);
					}
				}

				clients(0);
			}
		}

		new SolrTable(solrClientFactory, columns, filledColumnMapping, options);
	}
}