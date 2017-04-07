# solr-sql

solr-sql provides sql interfaces for solr cloud(http://lucene.apache.org/solr/), by which developers can operate on solr cloud via JDBC protocols.

On the same time, solr-sql is an Apache Calcite(see http://calcite.apache.org) adapter for solr.

solr-sql is written in Scala, which generates JVM byte codes like Java.

So, if you are a Java developer, do not hesitate to choose solr-sql, because it is easy to be referenced by Java code. Also the test cases are written in Java(see https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SolrSqlQueryTest.java).

If you are interested in source codes, manybe you need install a ScalaIDE(http://scala-ide.org/), or scala plugins for eclipse.

solr-sql uses Maven to manage libary dependencies, it is a normal Maven project.

# import solr-sql

use maven to import solr-sql:

	<dependency>
	  <groupId>com.github.bluejoe2008</groupId>
	  <artifactId>solr-sql</artifactId>
	  <version>0.9</version>
	</dependency>

# JDBC client code

example code:

	Properties info = new Properties();
	info.setProperty("lex", "JAVA");
	Connection connection = DriverManager.getConnection(
			"jdbc:calcite:model=src/java/test/model.json", info);

	Statement statement = connection.createStatement();
	String sql = "select * from docs where not (age>35 and name='bluejoe')";
	ResultSet resultSet = statement.executeQuery(sql);
	
this illustrates how to connect to a solr 'database' in JDBC client manners, the schema of 'database' is defined in file 'src/java/test/model.json'.

# run tests

https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SolrSqlQueryTest.java shows how to connect to a JDBC source of solr.

https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SqlTranslatorTest.java tests if the translation from SQL filters to Solr filters is right, like:

	Assert.assertEquals("age_i:{20 TO *}",
		trans("select * from docs where age>20"));
		


