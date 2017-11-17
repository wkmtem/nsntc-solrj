package com.nsntc.solrj.service;

import java.util.List;
import java.util.Map;

import com.nsntc.solrj.pojo.Foo;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.nsntc.solrj.pojo.Foo;

/**
 * SolrjService
 * @author wkm
 */
public class SolrjService {

    /**
     * 定义http的solr服务
     */
    private HttpSolrServer httpSolrServer;

    public SolrjService(HttpSolrServer httpSolrServer) {
        this.httpSolrServer = httpSolrServer;
    }

    /**
     * 新增
     * @param foo
     * @throws Exception
     */
    public void add(Foo foo) throws Exception {
        this.httpSolrServer.addBean(foo);
        this.httpSolrServer.commit();
    }

    /**
     * 修改
     * @param foo
     * @throws Exception
     */
    public void update(Foo foo) throws Exception {
        this.httpSolrServer.addBean(foo);
        this.httpSolrServer.commit();
    }

    /**
     * 删除id
     * @param id
     * @throws Exception
     */
    public void deleteById(String id) throws Exception {
        this.httpSolrServer.deleteById(id);
        this.httpSolrServer.commit();
    }

    /**
     * 删除ids
     * @param ids
     * @throws Exception
     */
    public void delete(List<String> ids) throws Exception {
        this.httpSolrServer.deleteById(ids);
        this.httpSolrServer.commit();
    }

    /**
     * 根据查询结果删除
     * @throws Exception
     */
    public void deleteByQuery() throws Exception{
        this.httpSolrServer.deleteByQuery("*:*");
        this.httpSolrServer.commit();
    }

    /**
     * 查询 高亮
     * @param keywords
     * @param page
     * @param rows
     * @return
     * @throws Exception
     */
    public List<Foo> search(String keywords, Integer page, Integer rows) throws Exception {
        /**
         * 构造搜索条件
         */
        SolrQuery solrQuery = new SolrQuery();
        /**
         * 搜索关键词
         */
        solrQuery.setQuery("title:" + keywords);
        /**
         * 设置分页:start=0从0开始，rows=5当前返回5条记录
         */
        solrQuery.setStart((Math.max(page, 1) - 1) * rows);
        solrQuery.setRows(rows);

        /**
         * 是否需要高亮
         */
        boolean isHighlighting = !StringUtils.equals("*", keywords) && StringUtils.isNotEmpty(keywords);

        /**
         * 高亮
         */
        if (isHighlighting) {
            /**
             * 开启高亮组件
             */
            solrQuery.setHighlight(true);
            /**
             * 高亮字段
             */
            solrQuery.addHighlightField("title");
            /**
             * 标记，高亮关键字前缀
             */
            solrQuery.setHighlightSimplePre("<em>");
            /**
             * 后缀
             */
            solrQuery.setHighlightSimplePost("</em>");
        }

        /**
         * 执行查询
         */
        QueryResponse queryResponse = this.httpSolrServer.query(solrQuery);
        List<Foo> foos = queryResponse.getBeans(Foo.class);
        if (isHighlighting) {
            /**
             * 将高亮的标题数据写回到数据对象中
             */
            Map<String, Map<String, List<String>>> map = queryResponse.getHighlighting();
            for (Map.Entry<String, Map<String, List<String>>> highlighting : map.entrySet()) {
                for (Foo foo : foos) {
                    if (!highlighting.getKey().equals(foo.getId().toString())) {
                        continue;
                    }
                    foo.setTitle(StringUtils.join(highlighting.getValue().get("title"), ""));
                    break;
                }
            }
        }
        return foos;
    }
}
