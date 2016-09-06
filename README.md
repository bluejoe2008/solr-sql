# solr-sql

solr-sql provides sql interfaces for solr cloud, by which developers can operate on solr cloud via JDBC protocols.

On the same time, it is an Calcite adapter for solr(see http://calcite.apache.org).

# JDBC client codes

example codes:

	Properties info = new Properties();
	info.setProperty("lex", "JAVA");
	Connection connection = DriverManager.getConnection(
			"jdbc:calcite:model=src/java/test/model.json", info);

	Statement statement = connection.createStatement();
	String sql = "select * from docs where not (age>35 and name='bluejoe')";
	ResultSet resultSet = statement.executeQuery(sql);
		

# table definition

below shows the example schema definition file model.json:


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
	
this defines a custom table named 'docs', below shows what arguments can be defined in the :

* solrServerURL: solr server url, e.g. 'http://bluejoe1:8983/solr/collection1'
* solrCollection: collection name, e.g. 'collection1'
* solrZkHosts: zookeeper hosts employed by solr cloud, e.g. 'bluejoe1:9983'
* columns: comma seperated column definitons, each column is describled in format <columnName columnTypeName>, e.g. 'id integer, name char, age integer',
* columnMapping: tells real names of fields in solr document for each column, each column mapping is describled in format <columnName->fieldNameInSolrDocument>, e.g. 'name->name_s, age->age_i'
  




