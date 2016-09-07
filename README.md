# solr-sql

solr-sql provides sql interfaces for solr cloud(http://lucene.apache.org/solr/), by which developers can operate on solr cloud via JDBC protocols.

On the same time, solr-sql is an Calcite adapter for solr(see http://calcite.apache.org).

# about project

solr-sql is written in Scala, which generates JVM byte codes like Java.

So, if you are a Java developer, do not hesitate to choose solr-sql, because it is easy to be referenced by Java code. Also the test cases are written in Java(see https://github.com/bluejoe2008/solr-sql/blob/master/src/java/test/SolrSqlQueryTest.java).

If you are interested in source codes, manybe you need install a ScalaIDE(http://scala-ide.org/), or scala plugins for eclipse.

solr-sql uses Maven to manage libary dependencies, it is a normal Maven project.

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
* columns: comma seperated column definitions, each column is describled in format <columnName columnTypeName>, e.g. 'id integer, name char, age integer',
* columnMapping: tells real names of fields in solr document for each column, each column mapping is describled in format <columnName->fieldNameInSolrDocument>, e.g. 'name->name_s, age->age_i'
  




