package org.apache.calcite.adapter.solr

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.immutable.Map

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.FilterableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument

class SolrTable(solrClientFactory: SolrClientFactory, columns: Map[String, SqlTypeName], columnMapping: Map[String, String], options: Map[String, String]) extends AbstractTable
		with ScannableTable with FilterableTable {
	val logger = Logger.getLogger(this.getClass);
	val pageSize = SolrTableConf.parseInt(options, SolrTableConf.PAGE_SIZE, "50");

	override def scan(root: DataContext) =
		{
			val solrQuery = new SolrQuery;
			solrQuery.setQuery("*:*");
			Linq4j.asEnumerable(new SolrQueryResults(solrClientFactory, solrQuery, pageSize));
		}

	override def getRowType(typeFactory: RelDataTypeFactory) =
		typeFactory.createStructType(
			columns.values.map { typeFactory.createSqlType(_) }.toList, columns.keys.toList);

	override def scan(root: DataContext, filters: java.util.List[RexNode]) =
		{
			logger.debug(s"filters: $filters");
			val solrQuery = buildSolrQuery(filters);
			Linq4j.asEnumerable(new SolrQueryResults(solrClientFactory, solrQuery, pageSize));
		};

	def buildEmptySolrQuery(): SolrQuery = {
		val solrQuery = new SolrQuery;
		solrQuery
	}

	//builds solr query according to sql filters
	def buildSolrQuery(filters: java.util.List[RexNode]): SolrQuery = {
		val solrQuery = new SolrQuery;

		if (filters.isEmpty())
			solrQuery.setQuery("*:*");
		else
			solrQuery.setQuery(new SqlFilter2SolrFilterTranslator(columnMapping.values.toArray).translate(filters.get(0)).toSolrQueryString())

		solrQuery
	}

	class SolrQueryResultsIterator(solrClientFactory: SolrClientFactory, solrQuery: SolrQuery, pageSize: Int = 20) extends java.util.Iterator[Array[Object]] {
		var startOfCurrentPage = 0;
		var rowIteratorWithinCurrentPage: java.util.Iterator[Array[Object]] = null;
		var totalCountOfRows = -1L;

		val mySolrQuery = solrQuery.getCopy();
		readNextPage();

		/**
		 * translates a solr document into a row
		 */
		def doc2Row(doc: SolrDocument): Array[Object] = {
			val row =
				for ((columnName, columnType) ← columns)
					yield fieldValue2ColumnValue(doc, columnMapping(columnName), columnType);

			row.toArray
		}

		def fieldValue2ColumnValue(doc: SolrDocument, fieldName: String, targetType: SqlTypeName): Object = {
			val value = doc.getFieldValue(fieldName);

			(value, targetType) match {
				case (null, _) ⇒ null;
				case (_: String, SqlTypeName.CHAR) ⇒ value;
				case (_: String, SqlTypeName.VARCHAR) ⇒ value;
				case (_: java.lang.Integer, SqlTypeName.INTEGER) ⇒ value;
				case (_: java.lang.Long, SqlTypeName.BIGINT) ⇒ value;
				case (_: java.lang.Float, SqlTypeName.FLOAT) ⇒ value;
				case (_: java.lang.Double, SqlTypeName.DOUBLE) ⇒ value;
				case (_: Date, SqlTypeName.DATE) ⇒ value;

				case (_, SqlTypeName.CHAR) ⇒ value.toString();
				case (_, SqlTypeName.VARCHAR) ⇒ value.toString();
				case (_, SqlTypeName.INTEGER) ⇒ Integer.valueOf(value.toString);
				case (_, SqlTypeName.BIGINT) ⇒ java.lang.Long.valueOf(value.toString);
				case (_, SqlTypeName.DATE) ⇒ new SimpleDateFormat("yyyy-mm-dd").parse(value.toString());
				case (_, _) ⇒ throw new SolrSqlException(s"unexpected value: $value, type $targetType required");
			}
		}

		def readNextPage(): Boolean = {
			if (totalCountOfRows < 0 || startOfCurrentPage < totalCountOfRows) {
				mySolrQuery.set("start", startOfCurrentPage);
				mySolrQuery.set("rows", pageSize);
				startOfCurrentPage += pageSize;
				logger.debug(s"executing solr query: $mySolrQuery");
				val rsp = solrClientFactory.getClient().query(mySolrQuery);
				val docs = rsp.getResults();
				totalCountOfRows = docs.getNumFound();
				logger.debug(s"numFound: $totalCountOfRows");
				val rows = docs.map { doc2Row };
				rowIteratorWithinCurrentPage = rows.iterator();
				true;
			}
			else {
				false;
			}
		}

		def hasNext() = { rowIteratorWithinCurrentPage.hasNext() || startOfCurrentPage < totalCountOfRows }
		def next() = {
			if (!rowIteratorWithinCurrentPage.hasNext()) {
				if (!readNextPage()) throw new NoSuchElementException();
			}

			rowIteratorWithinCurrentPage.next()
		}
	}

	class SolrQueryResults(solrClientFactory: SolrClientFactory, solrQuery: SolrQuery, pageSize: Int) extends java.lang.Iterable[Array[Object]] {
		def iterator() = new SolrQueryResultsIterator(solrClientFactory, solrQuery, pageSize)
	}
}


	