package com.zc;

import com.alibaba.fastjson.JSON;
import com.zc.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * description: es高级客户端测试 API
 * author: zhaocan
 * time: 2021/7/4 17:42
*/
@SpringBootTest
class ElasticsearchApiApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    //  测试索引的创建
    @Test
    void testCreateIndex() throws IOException {
        //  1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("zhaocan_index");
        //  2、客户端执行请求，请求后获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //  获取索引，只能判断是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("zhaocan_index");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

        System.out.println(exists);
    }

    //  测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("zhaocan_index");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //  测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //  1、创建对象
        User user = new User("赵灿", 23);
        //  2、创建请求
        IndexRequest request = new IndexRequest("zhaocan_index");

        //  3、设置规则
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //  4、将数据放入请求
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);

        //  5、客户端发送请求，获取响应结果
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);

        System.out.println(index.toString());
        System.out.println(index.status().toString());
    }

    //  获取文档，判断是否存在
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("zhaocan_index", "1");
        //  不获取返回的_source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //  排序字段
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println(exists);
    }

    //  获得文档的信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("zhaocan_index", "1");
        GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(documentFields.getSourceAsString()); //打印文档内容
        System.out.println(documentFields); //  返回的全部内容和命令是一样的
    }

    //  更新文档的信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("zhaocan_index", "1");
        updateRequest.timeout("1s");

        User user = new User("张三", 3);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(update.status());
    }

    //  删除文档记录
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("zhaocan_index", "1");
        deleteRequest.timeout("1s");

        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(delete.status());
    }

    //  批量查询
    @Test
    void testBluk() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("zhaocan1", 3));
        users.add(new User("zhaocan2", 3));
        users.add(new User("zhaocan3", 3));
        users.add(new User("zhaocan4", 3));
        users.add(new User("zhaocan5", 3));
        users.add(new User("zhaocan6", 3));
        users.add(new User("zhaocan7", 3));
        users.add(new User("zhaocan8", 3));

        //  批处理请求
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("zhaocan_index")
                            //.id(i+1+"")
                            .source(JSON.toJSONString(users.get(i)),XContentType.JSON)
            );
        }

        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        //  返回false是成功
        System.out.println(bulk.hasFailures());
    }

    //  查询
    //  SearchRequest：搜索请求
    //  SearchSourceBuilder：条件构造
    //  HighlightBuilder：构建高亮
    //  TermQueryBuilder：精确查询
    //  MatchAllQueryBuilder：匹配所有
    //  xxxQueryBuilder：对应之前的所有命令
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("zhaocan_index");

        //  构建搜索的条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //  查询条件，我们可以使用QueryBuilders工具类来实现
        //  QueryBuilders.termQuery：精确匹配
        //  QueryBuilders.matchAllQuery()：匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "zhaocan1");
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        builder.query(termQueryBuilder);

        builder.from(0);
        builder.size(10);
        builder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest source = searchRequest.source(builder);

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println("===================================");
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
