onsqlcodec
==========
This is the custom Solr codec for seamlessly integrating SolrCloud-based index with Oracle NoSQL key-value clustered storage.

The code was created using Solr 4.10.1 API and NoSQL 3.0 API versions and intended to use within SolrCloud cluster.
It's main purpose and advantage is to transparently relay all index-stored document fields to dedicated NoSQL storage 
with it's own replication and fault-tolerance strategy. This approach greatly reduces index size and speeds up 
all the index manipulations and merging, allowing use of Solr both as a NoSQL database and search engine at the same time.

The codec  relays stored fields to the NOSQL store while keeping all other index components in usual file-based storage layout
codec has been made with SolrCloud and NoSQL own fault tolarance usage in mind, hence it's tried to ignore wrote commands 
to NoSQL storage if index is being created at replica node which is not a Solr shard leader currently.
To build this codec, you will need to download Oracle NOSQL database open edition and copy two files into ./lib folder:
je.jar
kvstore.jar

Besides of these files, standard Lucene and Solr libraries are required:
lucene-analyzers-common-4.10.1.jar
lucene-codecs-4.10.1.jar
lucene-core-4.10.1.jar
lucene-queries-4.10.1.jar
lucene-queryparser-4.10.1.jar
slf4j-api-1.7.6.jar
solr-core-4.10.1.jar
solr-solrj-4.10.1.jar
zookeeper-3.4.5.jar

Copy them into the ./lib folder from Solr distribution, then run ant with supplied build.xml file

You will get onsqlcodec.jar in ./dist folder after succesfull compilation
Details on customizing your Solr installation to use this codec are given below:
1. Create Solr cores of your choice.
2. Edit solrconfig.xml file
  2.1 Copy supplementary libs, nesessary to run the codec, there will be 2 jars from Oracle NoSQL distribution and codec itself, 
      then add them to lib section, as shown below:
       <lib path="../onsqlcodec.jar" /> 
       <lib path="../kvstore.jar" /> 
       <lib path="../je.jar" /> 
  2.2 Replace default Directory implementation with custom one, as shown below:
  <directoryFactory name="DirectoryFactory" 
                    class="${solr.directoryFactory:dell.apps.lucene.codecs.onsql.ONSQLWrapperDirectoryFactory}">
  2.3 Replace default codecfactory with custom implementation as shown below:
  <codecFactory class="dell.apps.lucene.codecs.onsql.ONSQLCodecFactory"/>
3. Create kvstore.properties config file, residing in Solr core config folder, then add these properties to specify Oracle NoSQL connection details:
kvstore.name=<put_name_of_your_Oracle_NoSQL_instance_here>
kvstore.hosts=<put_comma_separated_list_of_host:port_connections_to_nosqlstore_here>
kvstore.doc_primary_key_fields=<put_list_of_document_field_names_to_uniquely_identify_doc_here>
4. Start Oracle NoSQL instance cluster, then start Solr cluster (or Solr single instance, if you do not have SolrCloud setup).

That's it, your new Solr instance ()cluster with NoSQL integration is ready for use.






