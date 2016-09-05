package org.apache.calcite.adapter.solr

import java.sql.SQLException

class SolrSqlException(msg: String, e: Throwable) extends SQLException(msg, e) {
	def this(msg: String) = this(msg, null)
}