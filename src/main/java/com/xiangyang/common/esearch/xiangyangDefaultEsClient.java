package com.xiangyang.common.esearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.util.StringUtils;

public class xiangyangDefaultEsClient implements ESClient{
	private final int SUCCESS_CODE = 200;
	
	private final int SUCCESS_CODE2 =201;//创建文档成功的时候回返回201 修改的时候回返回200

	@Override
	public Map<String, Object> getDocument(String index, String id) throws RuntimeException {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			if(StringUtils.isEmpty(id)) {
				throw new RuntimeException("ID IS NULL");
			}
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			GetRequest request = new GetRequest(index, id);
			GetResponse response = client.get(request,  RequestOptions.DEFAULT);
			client.close();
			if(!response.isExists()) {
				return new HashMap<String, Object>();
			}
			return response.getSource();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean indexDocument(String index, String id, Map<String, Object> doc) throws RuntimeException {
		RestHighLevelClient client =null;
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			if(StringUtils.isEmpty(id)) {
				throw new RuntimeException("ID IS NULL");
			}
			IndexRequest indexRequest = new IndexRequest(index)
				    .id(id).source(doc);
			EsClientConfig esClientConfig = new EsClientConfig();
			client = esClientConfig.client();
			IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
			int statusCode = response.status().getStatus();
			//result = updated 的时候 修改成功返回为200 为created的时候添加成功返回为201
			if(statusCode == SUCCESS_CODE||statusCode == SUCCESS_CODE2) {
				return true;
			}
			return false;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	public boolean updateDocument(String index, String id, Map<String, Object> doc)
			throws RuntimeException {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			if(StringUtils.isEmpty(id)) {
				throw new RuntimeException("ID IS NULL");
			}
			UpdateRequest updateRequest = new UpdateRequest(index, id).doc(doc);
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
			int statusCode = response.status().getStatus();
			if(statusCode == SUCCESS_CODE) {
				return true;
			}
			return false;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	public boolean deleteDocument(String index, String id) throws RuntimeException {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			if(StringUtils.isEmpty(id)) {
				throw new RuntimeException("ID IS NULL");
			}
			DeleteRequest deleteRequest = new DeleteRequest(index, id);
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
			int statusCode = response.status().getStatus();
			if(statusCode == 404) {
				throw new RuntimeException("文档不存在");
			}
			if(statusCode == SUCCESS_CODE) {
				return true;
			}
			return false;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public Long updateDocumentByQuery(String index,List<QueryBuilder> queryBuilders,Map<String, Object> params) {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			if(params.isEmpty()) {
				throw new RuntimeException("PARAMS IS NULL");
			}
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			UpdateByQueryRequest request =
			        new UpdateByQueryRequest(index);
			for(QueryBuilder query:queryBuilders) {
				request.setQuery(query);
			}
			StringBuilder idOrCode =new StringBuilder();
			for(String key:params.keySet()) {
				idOrCode.append("ctx._source.").append(key).append(" = params.").append(key).append(";");
			}
			Script script = new Script(ScriptType.INLINE, "painless", idOrCode.toString(), params);
			request.setScript(script);
			BulkByScrollResponse response =
			        client.updateByQuery(request, RequestOptions.DEFAULT);
			return response.getStatus().getUpdated();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public Long deleteDocumentByQuery(String index, List<QueryBuilder> queryBuilderList) {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			DeleteByQueryRequest request =
			        new DeleteByQueryRequest(index);
			for(QueryBuilder query:queryBuilderList) {
				request.setQuery(query);
			}
			BulkByScrollResponse response =
			        client.deleteByQuery(request, RequestOptions.DEFAULT);
			return response.getStatus().getDeleted();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	public Boolean createIndexAndMapping(String index,Map<String, Object> source) {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(this.indexExists(index)) {
				throw new RuntimeException("INDEX IS EXISTS");
			}
			CreateIndexRequest request = new CreateIndexRequest(index);
			request.mapping(source);
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
			return  response.isAcknowledged(); 
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	public Boolean deleteIndex(String index) {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			if(!this.indexExists(index)) {
				throw new RuntimeException("INDEX IS NOT EXISTS");
			}
			DeleteIndexRequest request = new DeleteIndexRequest(index);
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
			return deleteIndexResponse.isAcknowledged();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	public Boolean indexExists(String index) {
		try {
			if(StringUtils.isEmpty(index)) {
				throw new RuntimeException("INDEX IS NULL");
			}
			GetIndexRequest request = new GetIndexRequest(index);
			EsClientConfig esClientConfig = new EsClientConfig();
			RestHighLevelClient client = esClientConfig.client();
			boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
			return exists;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public static void main(String[] args) throws IOException {
//		EsClientConfig esClientConfig = new EsClientConfig();
//		RestHighLevelClient client = esClientConfig.client();
//		GetRequest request = new GetRequest("customer", "1");
//		GetResponse response = client.get(request,  RequestOptions.DEFAULT);
//		System.out.println(response);
//		System.out.println(response.isExists());
//		if(!response.isExists()) {
//			
//		}
//		System.out.println(response.getSource());
//		client.close();
		//新增测试
//		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		Map<String, Object> requestMap = new HashMap<>();
//		requestMap.put("id", "2");
//		requestMap.put("balance", "5000");
//		requestMap.put("customerId", "2");
//		requestMap.put("date",System.currentTimeMillis());
//		Boolean bl= defaultEsClient.indexDocument("account", "3", requestMap);
//		System.out.println(bl);
		//修改测试
//		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		Map<String, Object> requestMap = new HashMap<>();
//		requestMap.put("id", "3");
//		requestMap.put("balance", "400000");
//		requestMap.put("customerId", "2");
//		Boolean bl= defaultEsClient.updateDocument("account", "3", requestMap);
//		System.out.println(bl);
		
		//删除es测试
//		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		Boolean bl = defaultEsClient.deleteDocument("customer", "3");
//		System.out.println(bl);
		//获取多个索引
//		EsClientConfig esClientConfig = new EsClientConfig();
//		RestHighLevelClient client = esClientConfig.client();
//		MultiGetRequest request = new MultiGetRequest();
//		request.add(new MultiGetRequest.Item("customer", "2"));  
//		request.add(new MultiGetRequest.Item("account", "5")); 
//		MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
//		System.out.println(response.getResponses()[1].getResponse());
//		client.close();
		//根据查询条件更新值
//		EsClientConfig esClientConfig = new EsClientConfig();
//		RestHighLevelClient client = esClientConfig.client();
//		UpdateByQueryRequest request =
//		        new UpdateByQueryRequest("account");
//		request.setQuery(new TermQueryBuilder("customerId", 2));
//		request.setQuery(QueryBuilders.matchAllQuery());
//		Map<String, Object> params = new HashMap<>();
//		params.put("balance", "8000");
//		params.put("updateTime", System.currentTimeMillis());
//		String idOrCode ="";
//		for(String key:params.keySet()) {
//			idOrCode +="ctx._source."+key+" = params."+key+";";
//		}
//		System.out.println("这个是"+idOrCode);
//		Script script = new Script(ScriptType.INLINE, "painless", idOrCode, params);
//		request.setScript(script);
//		BulkByScrollResponse response =
//		        client.updateByQuery(request, RequestOptions.DEFAULT);
//		System.out.println(response.getStatus().getUpdated());
//		System.out.println(response);
//		client.close();
		//调用updateByQuery
//		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		List<QueryBuilder> queryBuilders = new ArrayList<QueryBuilder>();
//		queryBuilders.add(QueryBuilders.matchAllQuery());
//		Map<String, Object> params = new HashMap<>();
//		params.put("balance", "6000");
//		Long long1= defaultEsClient.updateByQuery("account", queryBuilders, params);
//		System.out.println(long1);
		
		
//		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		List<QueryBuilder> queryBuilders = new ArrayList<QueryBuilder>();
//		queryBuilders.add(new TermQueryBuilder("customerId", 2));
//		queryBuilders.add(QueryBuilders.matchAllQuery());
//		Long long1 = defaultEsClient.deleteByQuery("account", queryBuilders);
//		System.out.println(long1);
		
//		EsClientConfig esClientConfig = new EsClientConfig();
//		RestHighLevelClient client = esClientConfig.client();
//		DeleteByQueryRequest request =
//		        new DeleteByQueryRequest("account");
//		request.setQuery(QueryBuilders.termQuery("customerId", "2"));
//		BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
//		System.out.println(response.getStatus().getDeleted());
//		client.close();
		//创建索引映射
		xiangyangDefaultEsClient defaultEsClient = new xiangyangDefaultEsClient();
//		Map<String, Object> message = new HashMap<>();
//		message.put("type", "text");
//		Map<String, Object> intMap = new HashMap<>();
//		intMap.put("type", "integer");
//		Map<String, Object> dateMap = new HashMap<>();
//		dateMap.put("type", "date");
//		Map<String, Object> properties = new HashMap<>();
//		properties.put("message", message);
//		properties.put("title", message);
//		properties.put("age", intMap);
//		properties.put("createTime", dateMap);
//		Map<String, Object> mapping = new HashMap<>();
//		mapping.put("properties", properties);
//		defaultEsClient.createIndexAndMapping("ceshi4", new HashMap<String,Object>());
//	   System.out.println( defaultEsClient.deleteIndex("ceshi1"));
//		System.out.println(defaultEsClient.indexExists("ceshi1"));
	   
	   SearchRequest searchRequest = new SearchRequest("bank","customer");
	   SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
	   BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
	   boolQueryBuilder.must().add(QueryBuilders.termQuery("account_number", "2"));
	   boolQueryBuilder.must().add(QueryBuilders.termQuery("id", "2"));
//	   sourceBuilder.query(QueryBuilders.matchQuery("id", "2").fuzziness(Fuzziness.AUTO));
//	   sourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("account_number", "2")));
//	   QueryBuilder queryBuilder = null;
//	   queryBuilder= QueryBuilders.boolQuery().must(QueryBuilders.termQuery("account_number", "2"));
//	   queryBuilder= QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", "2"));
//	   sourceBuilder.query(QueryBuilders.termQuery("account_number", "2"));
//	   sourceBuilder.query(QueryBuilders.termQuery("id", "2"));
	   sourceBuilder.query(boolQueryBuilder);
	   EsClientConfig client = new EsClientConfig();
	   searchRequest.source(sourceBuilder);
	   SearchResponse searchResponse = client.client().search(searchRequest, RequestOptions.DEFAULT);
	   for(SearchHit hit:searchResponse.getHits().getHits()) {
		   System.out.println(hit);
	   }
	   
	}

	


}
