package com.xiangyang.common.esearch;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 
 *     es客户端
 *
 */
public interface ESClient {
	
	/**
	 * 获取文档
	 * 
	 * @param index 索引
	 * @param id 文档ID
	 * @return
	 * @throws RuntimeException
	 */
	public Map<String, Object> getDocument(String index, String id) throws RuntimeException;
	
	/**
	 * 索引覆盖文档
	 * 
	 * @param index
	 * @param id
	 * @param doc
	 * @return
	 * @throws RuntimeException
	 */
	public boolean indexDocument(String index, String id, Map<String, Object> doc) throws RuntimeException;
	
	
	/**
	 * 更新文档
	 * 
	 * @param index
	 * @param id
	 * @param doc
	 * @return
	 * @throws RuntimeException
	 */
	public boolean updateDocument(String index, String id, Map<String, Object> doc)
			throws RuntimeException;
	
	/**
	 * 删除文档
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 * @throws RuntimeException
	 */
	public boolean deleteDocument(String index, String id) throws RuntimeException;
	
	 /**
	  * 按照查询条件去更新es文档的数据
	  * @param index 索引
	  * @param queryBuilders 查询条件
	  * @param params 参数和值
	  * @return 返回更新的条数
	  */
    public Long updateByQuery(String index,List<QueryBuilder> queryBuilders,Map<String, Object> params);

    /**
     * 按照查询条件去删除es文档的数据
     * @param index 索引
     * @param queryBuilderList 查询条件
     * @return  返回删除的条数
     */
    public Long deleteByQuery(String index, List<QueryBuilder> queryBuilderList);
    
    public Map<String, Object> searchDocuments(String index,List<QueryBuilder> queryBuilders,Integer from,Integer size,
    		String sortField, SortOrder sortOrder,AggregationBuilder aggregationBuilder) throws RuntimeException;
}
