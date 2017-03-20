package org.apache.calcite.adapter.solr

import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlKind
import org.apache.log4j.Logger
import scala.collection.JavaConversions
import java.util.Properties
import org.apache.calcite.sql.fun.SqlCastFunction

/**
 * a solr filter is abstraction of solr query filter
 */
trait SolrFilter {
	def toSolrQueryString(): String;
}

/**
 * SqlFilter2SolrFilterTranslator translates a sql filter into a solr filter
 */
class SqlFilter2SolrFilterTranslator(solrFieldNames: Array[String]) {

	val logger = Logger.getLogger(this.getClass);

	private def translateColumn(ref: RexInputRef): String = {
		solrFieldNames(ref.getIndex);
	}

	def translate(node: RexNode): SolrFilter = {
		processUnrecognied(processNOT(translateSqlFilter2SolrFilter(node)))
	}

	private def trimColumnCast(node: RexNode) = {
		node match {
			case call: RexCall ⇒
				(call.op, call.operands.get(0)) match {
					case (_: SqlCastFunction, ref: RexInputRef) ⇒
						logger.debug(s"simplify $node => $ref");
						ref;
					case _ ⇒ call;
				}

			case _ ⇒ node
		}
	}

	private def translateSqlFilter2SolrFilter(node: RexNode) = {
		node match {
			case _: RexCall ⇒
				val left = trimColumnCast(node.asInstanceOf[RexCall].operands.get(0));
				val right = if (node.asInstanceOf[RexCall].operands.size() > 1) { trimColumnCast(node.asInstanceOf[RexCall].operands.get(1)); } else { null; }

				(node.getKind, left, right) match {
					case (SqlKind.AND, _, _) ⇒ new AndSolrFilter(translate(left), translate(right));
					case (SqlKind.OR, _, _) ⇒ new OrSolrFilter(translate(left), translate(right));
					case (SqlKind.IS_NULL, ref: RexInputRef, null) ⇒ new IsNullSolrFilter(translateColumn(ref));
					case (SqlKind.IS_NOT_NULL, ref: RexInputRef, null) ⇒ new NotNullSolrFilter(translateColumn(ref));
					case (SqlKind.GREATER_THAN, ref: RexInputRef, lit: RexLiteral) ⇒ new GtSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.GREATER_THAN, lit: RexLiteral, ref: RexInputRef) ⇒ new LeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LESS_THAN, ref: RexInputRef, lit: RexLiteral) ⇒ new LtSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LESS_THAN, lit: RexLiteral, ref: RexInputRef) ⇒ new GeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.GREATER_THAN_OR_EQUAL, ref: RexInputRef, lit: RexLiteral) ⇒ new GeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.GREATER_THAN_OR_EQUAL, lit: RexLiteral, ref: RexInputRef) ⇒ new LtSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LESS_THAN_OR_EQUAL, ref: RexInputRef, lit: RexLiteral) ⇒ new LeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LESS_THAN_OR_EQUAL, lit: RexLiteral, ref: RexInputRef) ⇒ new GtSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.EQUALS, lit: RexLiteral, ref: RexInputRef) ⇒ new EqualsSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.EQUALS, ref: RexInputRef, lit: RexLiteral) ⇒ new EqualsSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LIKE, lit: RexLiteral, ref: RexInputRef) ⇒ new LikeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.LIKE, ref: RexInputRef, lit: RexLiteral) ⇒ new LikeSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.NOT_EQUALS, lit: RexLiteral, ref: RexInputRef) ⇒ new NotEqualsSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.NOT_EQUALS, ref: RexInputRef, lit: RexLiteral) ⇒ new NotEqualsSolrFilter(translateColumn(ref), lit.getValue2);
					case (SqlKind.NOT, _, null) ⇒ new NotSolrFilter(translate(left))
					case _ ⇒
						logger.debug(s"unable to translate filter: $node");
						new UnrecognizedSolrFilter();
				}
		}
	}

	def processNOT(filter: SolrFilter): SolrFilter = {
		filter match {
			case AndSolrFilter(left, right) ⇒ new AndSolrFilter(processNOT(left), processNOT(right));
			case OrSolrFilter(left, right) ⇒ new OrSolrFilter(processNOT(left), processNOT(right));
			case NotSolrFilter(left) ⇒ {
				left match {
					case AndSolrFilter(left, right) ⇒ new OrSolrFilter(processNOT(new NotSolrFilter(left)), processNOT(new NotSolrFilter(right)));
					case OrSolrFilter(left, right) ⇒ new AndSolrFilter(processNOT(new NotSolrFilter(left)), processNOT(new NotSolrFilter(right)));
					case NotSolrFilter(left) ⇒ processNOT(left);
					case GtSolrFilter(column, value) ⇒ new LeSolrFilter(column, value);
					case GeSolrFilter(column, value) ⇒ new LtSolrFilter(column, value);
					case LtSolrFilter(column, value) ⇒ new GeSolrFilter(column, value);
					case LeSolrFilter(column, value) ⇒ new GtSolrFilter(column, value);
					case EqualsSolrFilter(column, value) ⇒ new NotEqualsSolrFilter(column, value);
					case NotEqualsSolrFilter(column, value) ⇒ new EqualsSolrFilter(column, value);
					case NotNullSolrFilter(column) ⇒ new IsNullSolrFilter(column);
					case IsNullSolrFilter(column) ⇒ new NotNullSolrFilter(column);
				}
			}
			case _ ⇒ filter;
		}
	}

	def processUnrecognied(filter: SolrFilter): SolrFilter = {
		filter match {
			case AndSolrFilter(_: UnrecognizedSolrFilter, _: UnrecognizedSolrFilter) ⇒ new UnrecognizedSolrFilter();
			case AndSolrFilter(left, _: UnrecognizedSolrFilter) ⇒ left;
			case AndSolrFilter(_: UnrecognizedSolrFilter, right) ⇒ right;
			case OrSolrFilter(_: UnrecognizedSolrFilter, _) ⇒ new UnrecognizedSolrFilter();
			case OrSolrFilter(_, _: UnrecognizedSolrFilter) ⇒ new UnrecognizedSolrFilter();
			case _ ⇒ filter;
		}
	}
}

class UnrecognizedSolrFilter extends SolrFilter {
	override def toSolrQueryString() = {
		"*:*";
	}
}

case class AndSolrFilter(left: SolrFilter, right: SolrFilter) extends SolrFilter {
	override def toSolrQueryString() = {
		s"(${left.toSolrQueryString}) AND (${right.toSolrQueryString})";
	}
}

case class NotSolrFilter(left: SolrFilter) extends SolrFilter {
	override def toSolrQueryString() = {
		throw new SolrSqlException(s"should never be called: $left");
	}
}

case class OrSolrFilter(left: SolrFilter, right: SolrFilter) extends SolrFilter {
	override def toSolrQueryString() = {
		s"(${left.toSolrQueryString}) OR (${right.toSolrQueryString})";
	}
}

case class GtSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:{$value TO *}";
	}
}

case class NotNullSolrFilter(attributeName: String) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:*";
	}
}

case class IsNullSolrFilter(attributeName: String) extends SolrFilter {
	override def toSolrQueryString() = {
		s"NOT $attributeName:*";
	}
}

case class EqualsSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:$value";
	}
}

case class LikeSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	val value2 = value.asInstanceOf[String].replaceAllLiterally("%", "*").replaceAll("_", "?");
	override def toSolrQueryString() = {
		s"$attributeName:$value2";
	}
}

case class NotEqualsSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"NOT $attributeName:$value";
	}
}

case class GeSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:[$value TO *]";
	}
}

case class LeSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:[* TO $value]";
	}
}

case class LtSolrFilter(attributeName: String, value: Object) extends SolrFilter {
	override def toSolrQueryString() = {
		s"$attributeName:{* TO $value}";
	}
}