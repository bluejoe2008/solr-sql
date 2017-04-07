# solr-sql

solr-sql provides sql interfaces for solr cloud(http://lucene.apache.org/solr/), by which developers can operate on solr cloud via JDBC protocols.

On the same time, solr-sql is an Apache Calcite(see http://calcite.apache.org) adapter for solr.

<img src="https://github.com/bluejoe2008/solr-sql/blob/master/docs/solr-sql-arch.jpg?raw=true" width=500/>

# about project

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

# table definition

the file 'src/java/test/model.json' shows an example schema definition as below:


	{
		version: '1.0',
		defaultSchema: 'solr',
		schemas:
		[
			{
				name: 'solr',
				tables:
				[
					{
						name: 'docs',
						type: 'custom',
						factory: 'org.apache.calcite.adapter.solr.SolrTableFactory',
						operand:
						{
							solrServerURL: 'http://bluejoe1:8983/solr/collection1',
							solrCollection: 'collection1',	
							//solrZkHosts: 'bluejoe1:9983',
							columns:'id integer, name char, age integer',
							columnMapping: 'name->name_s, age->age_i'
						}
					}
				]
			}
		]
	}
	
this defines a custom table named 'docs', several arguments can be defined in the operand field:

* solrServerURL: solr server url, e.g. 'http://bluejoe1:8983/solr/collection1'
* solrCollection: collection name, e.g. 'collection1'
* solrZkHosts: zookeeper hosts employed by solr cloud, e.g. 'bluejoe1:9983'
* columns: comma seperated column definitions, each column is describled in format 'column_name column_type_name', e.g. 'id integer, name char, age integer'
* columnMapping: comma seperated column mappings, each column mapping is describled in format 'columnName->field_name_in_solr_document', e.g. 'name->name_s, age->age_i'
* pageSize: solr-sql does not retrieve all results on querying, for example, it only retrieves first 50 results, if the sql engine requests for more, it retrieves for next 50 results. pageSize defines the size of each query, default value is 50.

# run tests

https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SolrSqlQueryTest.java shows how to connect to a JDBC source of solr.

https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SqlTranslatorTest.java tests if the translation from SQL filters to Solr filters is right, like:

	Assert.assertEquals("age_i:{20 TO *}",
		trans("select * from docs where age>20"));
		
# SqlFilter2SolrFilterTranslator

a SqlFilter2SolrFilterTranslator translates a SQL filter to a Solr filter. a SQL filter is represented by a Calcite RexNode object, and a solr filter is represented by a SolrFilter object. There are serveral SolrFilters defined in this project:

* AndSolrFilter
* NotSolrFilter
* OrSolrFilter
* GtSolrFilter
* NotNullSolrFilter
* IsNullSolrFilter
* EqualsSolrFilter
* LikeSolrFilter
* NotEqualsSolrFilter
* GeSolrFilter
* LeSolrFilter
* LtSolrFilter
* UnrecognizedSolrFilter

