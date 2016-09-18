import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrSqlQueryTest
{
	@BeforeClass
	public static void setup() throws Exception
	{
		// writes some documents for test
		setupSolrDocuments();
	}

	private static void setupSolrDocuments() throws SolrServerException,
			IOException
	{
		HttpSolrClient client = new HttpSolrClient(
				"http://bluejoe1:8983/solr/collection1");
		client.deleteByQuery("*:*");
		insertDocument(client, 1, "bluejoe", 38);
		insertDocument(client, 2, "even", 35);
		insertDocument(client, 3, "alex", 8);
		client.commit();
		client.close();
	}

	private static void insertDocument(HttpSolrClient client, int id,
			String name, int age) throws SolrServerException, IOException
	{
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", id);
		doc.addField("name_s", name);
		doc.addField("age_i", age);

		client.add(doc);
	}

	@Test
	public void test1() throws Exception
	{
		Assert.assertEquals(1,
				query("select * from docs where age>35 and name='bluejoe'")
						.size());
	}

	@Test
	public void test2() throws Exception
	{
		Assert.assertEquals(3, query("select * from docs limit 10").size());
	}

	@Test
	public void test3() throws Exception
	{
		Assert.assertEquals(1, query("select * from docs where age<35").size());
	}

	@Test
	public void test4() throws Exception
	{
		Assert.assertEquals(1, query("select * from docs where age>35").size());
	}

	@Test
	public void test5() throws Exception
	{
		Assert.assertEquals(2, query("select * from docs where age>=35").size());
	}

	@Test
	public void test6() throws Exception
	{
		Assert.assertEquals(2, query("select * from docs where age<=35").size());
	}

	@Test
	public void test7() throws Exception
	{
		Assert.assertEquals(2, query("select * from docs where not (age>35)")
				.size());
	}

	@Test
	public void test8() throws Exception
	{
		Assert.assertEquals(
				2,
				query(
						"select * from docs where not (age>35 and name='bluejoe')")
						.size());
	}

	@Test
	public void test9() throws Exception
	{
		Assert.assertEquals(2,
				query("select * from docs where age>35 or name='even'").size());
	}

	public List<Map<String, Object>> query(String sql) throws Exception
	{
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		Connection connection = DriverManager.getConnection(
				"jdbc:calcite:model=src/java/test/model.json", info);

		Statement statement = connection.createStatement();

		System.out.println("executing sql: " + sql);
		ResultSet resultSet = statement.executeQuery(sql);

		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		while (resultSet.next())
		{
			Map<String, Object> row = new HashMap<String, Object>();
			rows.add(row);

			for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
			{
				row.put(resultSet.getMetaData().getColumnName(i),
						resultSet.getObject(i));
			}
		}

		resultSet.close();
		statement.close();
		connection.close();

		System.out.println(rows);
		return rows;
	}
}
