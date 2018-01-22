package es.sample_codes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

public class EsUtils {
	
	/**
	 * delete index
	 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.x/java-admin-indices.html
	 * 
	 * @param client
	 */
	public static boolean deleteIndex(Client client, String index) {
		boolean deleted = false;
		
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		
		//DeleteResponse response = indicesAdminClient.prepareDelete("twitter", "tweet", "1").get();
		//GetResponse response = indicesAdminClient.prepareGet("twitter", "tweet", "1").get();
		
		if (indicesAdminClient.prepareExists(index).execute().actionGet().isExists()) {
			DeleteIndexResponse deleteIndexResponse = indicesAdminClient.delete(new DeleteIndexRequest(index))
					.actionGet();
			System.out.println("delete index :" + deleteIndexResponse.isAcknowledged());
			deleted = deleteIndexResponse.isAcknowledged();
		}		
		
		
		return deleted;
	}
	
	/**
	 * create index with mapping 
	 * @param client
	 * @param index
	 * @param type
	 * @param source	 
	 *  
	 * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     * 
     * sample: EsUtils.createIndexWithMapping(client, index, type, "field1", "type=text", "field2", "type=keyword");
     * 
     * @return
	 */
	public static boolean createIndexWithMapping(Client client, String index, String type, Object... source) {
		boolean created = false;
		CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(index).addMapping(type, source).get();
		created = createIndexResponse.isAcknowledged();
		System.out.println("created index :" + created);
 
		return created;
		
	}
	
	/**
	 * to update an existing mapping, can not update `type`
	 * @param client
	 * @param index
	 * @param type
	 * @param source
	 * 
	 * e.g.  to add a new field: 
	 *                    EsUtils.updateExistingMapping(client, index, type, "field3", "type=integer");
	 * 
	 * @return
	 */
	public static boolean updateExistingMapping (Client client, String index, String type, Object... source){
		
		boolean updated = false;
		PutMappingResponse putMappingResponse  = client.admin().indices().preparePutMapping(index)   
        .setType(type)                                
        .setSource(source)
        .get();
		updated = putMappingResponse.isAcknowledged();
		
		System.out.println("updated index :" + updated);
		return updated;
	}
	
	
	/**
	 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.x/java-docs-index.html
	 
	 * insert 1 record.
	 * @param client
	 * @param index
	 * @param type
	 * @param jsonContent
	 * 
	 * e.g.  
	 *     String json = "{" +
			        "\"field1\":\"kimchy\"," +
			        "\"field2\":\"2013\"," +
			        "\"field3\":\"99\"" +
			    "}";
			EsUtils.insertRecord(client, index, type, json);
			
	 * @return  CREATED for success.
	 */
	public static String insertRecord(final Client client,String index, String type,  String jsonContent){
		
		String resultStr = "failed";
		

		IndexResponse response = client.prepareIndex(index, type)
		        .setSource(jsonContent, XContentType.JSON)
		        .get();
		
		/**
		// Index name
		String _index = response.getIndex();
		// Type name
		String _type = response.getType();
		// Document ID (generated or not)
		String _id = response.getId();
		// Version (if it's the first time you index this document, you will get: 1)
		long _version = response.getVersion();
		*/
		// status has stored current instance statement.
		RestStatus status = response.status();
		
		resultStr = status.toString();
		
		System.out.println("status:" + status.toString());
		
		return resultStr;
		//_id: jbRAHmEBjKSLuz5n9ZBG,  status:CREATED
	}
	
	
	/**
	 * BULK insert records to index
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param records
	 * 
	 * sample codes:
	 * 
	  		List<Map<String, ?>> list = new ArrayList<Map<String, ?>>();
			
			Map s11 = new LinkedHashMap();
			s11.put("field1", "Think in java");
			s11.put("field2", "美国"); 
			s11.put("field3", 108);
			list.add(s11);

			Map s12 = new LinkedHashMap();
			s12.put("field1", "Head First Java");
			s12.put("field2", "英国"); 
			s12.put("field3", 54); 		    
		    list.add(s12);
		    
			EsUtils.bulkInsert(client, index, type, list);
	 * @return
	 */
	public static String bulkInsert(final Client client,String index, String type,  List<Map<String, ?>>  records){
		String resultStr = "failed";
		
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
	    //BulkRequestBuilder bulkRequestBuilder = 
	    Iterator it1 = records.iterator();
	    while(it1.hasNext()){
	      System.out.println();
	      
	      bulkRequest.add(client.prepareIndex(index, type).setSource((Map<String,?>)it1.next()).setOpType(IndexRequest.OpType.INDEX));
	      //.add(client.prepareIndex(index, type).setId("11").setSource(s11).setOpType(IndexRequest.OpType.INDEX)
	    }
	     
	    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		 
		 
		if (bulkResponse.hasFailures()) {
			resultStr = bulkResponse.buildFailureMessage();			 
		} else {
			System.out.println(bulkResponse.status());			
			resultStr = "OK";
		}
		
		return resultStr;
		
	}
	 
	
	/**
	 * 
	 * @param client
	 * @param fullDestPathAndName - full path with file name, if exists, override it.
	 * @param index
	 * @param types
	 * 
	 * sample codes: 
	 *           EsUtils.genJson2File(client, "d:/.kibana", ".kibana", "doc");
	 */
	public static boolean genJson2File(final Client client, String fullDestPathAndName,String index, String... types) {
		boolean flag = true;
		SearchResponse response = client.prepareSearch(index).setTypes(types).setQuery(QueryBuilders.matchAllQuery())
				.setExplain(true).execute().actionGet();

		try {
			File file = new File(fullDestPathAndName);
 
			if (file.exists()) {
				file.delete();
			}

			file = new File(fullDestPathAndName);

			file.createNewFile();

	 
			PrintWriter pfp = new PrintWriter(file, "UTF-8"); 
			for (SearchHit searchHit : response.getHits()) {
				pfp.print(searchHit.getSourceAsString() + "\n");				
			}
			pfp.close();

		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}
		
		return flag;
	}
	
	/**
	 * 
	 * @param client
	 * @param fullDestPathAndName
	 * @param index
	 * @param type
	 * @return
	 */
	public static boolean importJsonfile(final Client client, String fullDestPathAndName, String index, String type){
		boolean flag = true;
		
		FileReader fileReader = null;
		BufferedReader br = null;  
		try {
			String jsonContent = "";  
			
			fileReader = new FileReader(fullDestPathAndName); 
            br = new BufferedReader(fileReader);  
            while ((jsonContent = br.readLine()) != null) {  
 
                if (!jsonContent.trim().equals("")) {  
                	insertRecord(client, index, type, jsonContent);
                	
                }  
            }  
			
			
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {  
            try {                
                br.close();                  
                fileReader.close();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
		
		return flag;
	}

}
