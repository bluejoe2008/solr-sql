import java.util.Properties;

import junit.framework.Assert;

import org.apache.calcite.adapter.solr.SqlFilter2SolrFilterTranslator;
import org.apache.calcite.jdbc.CalcitePrepare.Query;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlTranslatorTest
{
	static SqlFilter2SolrFilterTranslator _trans;

	@BeforeClass
	public static void setup()
	{
		Properties columns = new Properties();
		columns.put("name", "name_s");
		columns.put("age", "age_i");

		_trans = new SqlFilter2SolrFilterTranslator(columns);
	}

	@Test
	public void test1()
	{
		Assert.assertEquals("*:*", trans("select * from docs"));
		Assert.assertEquals("age_i:[20 TO ]", trans("select * from docs where age>20"));
	}

	private String trans(String sql)
	{
		return _trans.translate(null).toSolrQueryString();
	}
}
