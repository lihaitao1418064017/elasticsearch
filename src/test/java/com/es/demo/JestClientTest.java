package com.es.demo;

import com.es.demo.entity.Article;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.*;
import io.searchbox.indices.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author LiHaitao
 * @description JestClientTest:
 * @date 2019/7/11 15:18
 **/
@SuppressWarnings("all")

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = DemoApplication.class)
public class JestClientTest {

    @Autowired
    private JestClient jestClient;

    Article article1 = new Article(1, "elasticsearch1", "系统版本：centos 7.4\n" +
            "ES版本：5.6.6\n" +
            "x-pack版本：5.6.6\n" +
            "x-pack5.6.6文档：https://www.elastic.co/guide/en/x-pack/5.6/index.html", "https://www.elastic.co/guide/en/x-pack/5.6/index.html", Calendar.getInstance().getTime(), "内网1", "匿名1");

    Article article2 = new Article(2, "elasticsearch2", "系统版本：centos 7.4\n" +
            "ES版本：5.6.6\n" +
            "x-pack版本：5.6.6\n" +
            "x-pack5.6.6文档：https://www.elastic.co/guide/en/x-pack/5.6/index.html", "https://www.elastic.co/guide/en/x-pack/5.6/index.html", Calendar.getInstance().getTime(), "内网2", "匿名2");

    Article article3 = new Article(3, "elasticsearch3", "系统版本：centos 7.4\n" +
            "ES版本：5.6.6\n" +
            "x-pack版本：5.6.6\n" +
            "x-pack5.6.6文档：https://www.elastic.co/guide/en/x-pack/5.6/index.html", "https://www.elastic.co/guide/en/x-pack/5.6/index.html", Calendar.getInstance().getTime(), "内网3", "匿名3");
    Article article4 = new Article(4, "elasticsearch4", "系统版本：centos 7.4\n" +
            "ES版本：5.6.6\n" +
            "x-pack版本：5.6.6\n" +
            "x-pack5.6.6文档：https://www.elastic.co/guide/en/x-pack/5.6/index.html", "https://www.elastic.co/guide/en/x-pack/5.6/index.html", Calendar.getInstance().getTime(), "内网4", "匿名4");


    /**
     * @Description: 创建连接
     * @Author: Lihaitao
     * @Date: 2019/7/11 15:23
     */
   /* @Before
    public void createJestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://47.99.216.57:9300")
                .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
                .multiThreaded(true)
                .readTimeout(10000)
                .build());
        jestClient = factory.getObject();


    }*/

    /**
     * @Description: 创建索引
     * @Author: Lihaitao
     * @Date: 2019/7/11 15:24
     * @UpdateUser:
     * @UpdateRemark:
     */
    @Test
    public void createIndex() throws IOException {
        Index index1 = new Index.Builder(article1).index("article").type("article").build();

        JestResult jestResult1 = jestClient.execute(index1);
        System.out.println(jestResult1.getJsonString());

    }

    /**
     * 批量插入
     *
     * @throws IOException
     */

    @Test
    public void batchInsert() throws IOException {
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("article")
                .defaultType("article")
                .addAction(Arrays.asList(
                        new Index.Builder(article1).build(),
                        new Index.Builder(article2).build(),
                        new Index.Builder(article3).build(),
                        new Index.Builder(article4).build()
                )).build();
        jestClient.execute(bulk);

    }

    /**
     * 创建查询
     *
     * @throws IOException
     */
    @Test
    public void createSearch() throws IOException {
        String queryString = "sssss";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(queryString));

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("article")
                .build();
        SearchResult result = jestClient.execute(search);
        System.out.println("本次查询共查到：" + result.getTotal() + "篇文章！");
        List<SearchResult.Hit<Article, Void>> hits = result.getHits(Article.class);
        for (SearchResult.Hit<Article, Void> hit : hits) {
            Article source = hit.source;
            //获取高亮后的内容
            Map<String, List<String>> highlight = hit.highlight;
            List<String> titlelist = highlight.get("title");//高亮后的title
            if (titlelist != null) {
                source.setTitle(titlelist.get(0));
            }
            List<String> contentlist = highlight.get("content");//高亮后的content
            if (contentlist != null) {
                source.setContent(contentlist.get(0));
            }
            System.out.println("标题：" + source.getTitle());
            System.out.println("内容：" + source.getContent());
            System.out.println("url：" + source.getUrl());
            System.out.println("来源：" + source.getSource());
            System.out.println("作者：" + source.getAuthor());
        }
    }


    /**
     * 查询全部
     *
     * @throws Exception
     */
    @Test
    public void searchAll() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("article")
                .build();
        SearchResult result = jestClient.execute(search);
        System.out.println("本次查询共查到：" + result.getTotal() + "篇文章！");
        List<SearchResult.Hit<Article, Void>> hits = result.getHits(Article.class);
        for (SearchResult.Hit<Article, Void> hit : hits) {
            Article source = hit.source;
            System.out.println("标题：" + source.getTitle());
            System.out.println("内容：" + source.getContent());
            System.out.println("url：" + source.getUrl());
            System.out.println("来源：" + source.getSource());
            System.out.println("作者：" + source.getAuthor());
        }
    }

    /**
     * Suggestion,联想
     *
     * @throws Exception
     */
    @Test
    public void suggest() throws Exception {
        String suggestionName = "my-suggestion";
        Suggest suggest = new Suggest.Builder("{" +
                "  \"" + suggestionName + "\" : {" +
                "    \"text\" : \"the amsterdma meetpu\"," +
                "    \"term\" : {" +
                "      \"field\" : \"body\"" +
                "    }" +
                "  }" +
                "}").build();
        SuggestResult suggestResult = jestClient.execute(suggest);
        System.out.println(suggestResult.isSucceeded());
        List<SuggestResult.Suggestion> suggestionList = suggestResult.getSuggestions(suggestionName);
        System.out.println(suggestionList.size());
        for (SuggestResult.Suggestion suggestion : suggestionList) {
            System.out.println(suggestion.text);
        }
    }


    /**
     * 获取Document
     *
     * @param index
     * @param type
     * @param id
     * @throws Exception
     */
    @Test
    public void getDocument() throws Exception {
        String index = "";
        String type = "";
        String id = "";
        Get get = new Get.Builder(index, id).type(type).build();
        JestResult result = jestClient.execute(get);
        Article article = result.getSourceAsObject(Article.class);
        System.out.println(article.getTitle() + "," + article.getContent());
    }

    /**
     * 删除Document
     *
     * @param index
     * @param type
     * @param id
     * @throws Exception
     */
    @Test
    public void deleteDocument() throws Exception {
        String index = "";
        String type = "";
        String id = "";
        Delete delete = new Delete.Builder(id).index(index).type(type).build();
        JestResult result = jestClient.execute(delete);
        System.out.println(result.getJsonString());
    }


    /**
     * 更新Document
     *
     * @param index
     * @param type
     * @param id
     * @throws Exception
     */
    @Test
    public void updateDocument() throws Exception {
        String index = "";
        String type = "";
        String id = "";
        Article article = new Article();
        article.setId(Integer.parseInt(id));
        article.setTitle("中国3颗卫星拍到阅兵现场高清照");
        article.setContent("据中国资源卫星应用中心报道，9月3日，纪念中国人民抗日战争暨世界反法西斯战争胜利70周年大阅兵在天安门广场举行。资源卫星中心针对此次盛事，综合调度在轨卫星，9月1日至3日连续三天持续观测首都北京天安门附近区域，共计安排5次高分辨率卫星成像。在阅兵当日，高分二号卫星、资源三号卫星及实践九号卫星实现三星联合、密集观测，捕捉到了阅兵现场精彩瞬间。为了保证卫星准确拍摄天安门及周边区域，提高数据处理效率，及时制作合格的光学产品，资源卫星中心运行服务人员从卫星观测计划制定、复核、优化到系统运行保障、光学产品图像制作，提前进行了周密部署，并拟定了应急预案，为圆满完成既定任务奠定了基础。");
        article.setPubdate(new Date());
        article.setAuthor("匿名");
        article.setSource("新华网");
        article.setUrl("http://news.163.com/15/0909/07/B32AGCDT00014JB5.html");
        String script = "{" +
                "    \"doc\" : {" +
                "        \"title\" : \"" + article.getTitle() + "\"," +
                "        \"content\" : \"" + article.getContent() + "\"," +
                "        \"author\" : \"" + article.getAuthor() + "\"," +
                "        \"source\" : \"" + article.getSource() + "\"," +
                "        \"url\" : \"" + article.getUrl() + "\"," +
                "        \"pubdate\" : \"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(article.getPubdate()) + "\"" +
                "    }" +
                "}";
        Update update = new Update.Builder(script).index(index).type(type).id(id).build();
        JestResult result = jestClient.execute(update);
        System.out.println(result.getJsonString());
    }


    /**
     * 将删除所有的索引
     *
     * @throws Exception
     */
    @Test
    public void deleteIndex() throws Exception {

        DeleteIndex deleteIndex = new DeleteIndex.Builder("article").build();
        JestResult result = jestClient.execute(deleteIndex);
        System.out.println(result.getJsonString());
    }


    /**
     * 清缓存
     *
     * @throws Exception
     */
    @Test
    public void clearCache() throws Exception {

        ClearCache closeIndex = new ClearCache.Builder().build();
        JestResult result = jestClient.execute(closeIndex);
        System.out.println(result.getJsonString());
    }


    /**
     * 关闭索引
     *
     * @throws Exception
     */
    @Test
    public void closeIndex() throws Exception {

        CloseIndex closeIndex = new CloseIndex.Builder("article").build();
        JestResult result = jestClient.execute(closeIndex);
        System.out.println(result.getJsonString());
    }

    /**
     * 优化索引
     *
     * @throws Exception
     */
    @Test
    public void optimize() throws Exception {

        Optimize optimize = new Optimize.Builder().build();
        JestResult result = jestClient.execute(optimize);
        System.out.println(result.getJsonString());
    }

    /**
     * 刷新索引
     *
     * @throws Exception
     */
    @Test
    public void flush() throws Exception {

        Flush flush = new Flush.Builder().build();
        JestResult result = jestClient.execute(flush);
        System.out.println(result.getJsonString());
    }

    /**
     * 判断索引目录是否存在
     *
     * @throws Exception
     */
    @Test
    public void indicesExists() throws Exception {

        IndicesExists indicesExists = new IndicesExists.Builder("article").build();
        JestResult result = jestClient.execute(indicesExists);
        System.out.println(result.getJsonString());
    }

    /**
     * 查看节点信息
     *
     * @throws Exception
     */
    @Test
    public void nodesInfo() throws Exception {

        NodesInfo nodesInfo = new NodesInfo.Builder().build();
        JestResult result = jestClient.execute(nodesInfo);
        System.out.println(result.getJsonString());
    }


    /**
     * 查看集群健康信息
     *
     * @throws Exception
     */
    @Test
    public void health() throws Exception {

        Health health = new Health.Builder().build();
        JestResult result = jestClient.execute(health);
        System.out.println(result.getJsonString());
    }

    /**
     * 节点状态
     *
     * @throws Exception
     */
    @Test
    public void nodesStats() throws Exception {
        NodesStats nodesStats = new NodesStats.Builder().build();
        JestResult result = jestClient.execute(nodesStats);
        System.out.println(result.getJsonString());
    }

}
