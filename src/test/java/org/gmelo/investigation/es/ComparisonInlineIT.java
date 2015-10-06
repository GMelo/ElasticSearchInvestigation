package org.gmelo.investigation.es;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.gmelo.investigation.model.Address;
import org.gmelo.investigation.model.Customer;
import org.gmelo.investigation.model.Telephone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by gmelo on 03/10/15.
 */
public class ComparisonInlineIT {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;
    private String indexName = "named_index";
    private final Logger logger = LoggerFactory.getLogger(ComparisonInlineIT.class);

    @Before
    public void setUp() throws InterruptedException {
        client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));

        Thread.sleep(900);
    }

    @Test
         public void queryWithJapaneseAnalyserOnField() throws InterruptedException {
        try {
            client.admin().indices().refresh(new RefreshRequest(indexName));
            final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(ImmutableSettings.settingsBuilder()
                            .loadFromSource(jsonBuilder()
                                    .startObject()
                                    .startObject("analysis")
                                            //
                                    .startObject("tokenizer")
                                    .startObject("kuromoji_user_dict")
                                    .field("type", "kuromoji_tokenizer")
                                    .field("mode", "search")
                                    .field("discard_punctuation", "false")
                                    .endObject()
                                    .endObject()
                                            //
                                    .startObject("analyzer")
                                    .startObject("jpn_analyser")
                                    .field("type", "custom")
                                    .field("tokenizer", "kuromoji_user_dict")
                                    .endObject()
                                    .endObject()
                                            //
                                    .endObject()
                                    .endObject().string()));

            createIndexRequestBuilder.execute().actionGet();
            //if a index had properties
            XContentBuilder xbMapping =
                    jsonBuilder()
                            .startObject()
                            .startObject(indexName)
                            .startObject("properties")
                            .startObject("source")
                            .field("type", "string")
                            .endObject()
                            .startObject("firstName")
                            .field("type", "string")
                            .field("analyzer", "jpn_analyser")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject();

            client.admin().indices()
                    .preparePutMapping(indexName)
                    .setType(indexName)
                    .setSource(xbMapping)
                    .execute().get();

        } catch (Exception e) {
            logger.error("Error creating index {}", indexName);
            logger.error(e.getMessage());
        }


        ObjectMapper objectMapper = new ObjectMapper();

        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "開発者ジャワ", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet);
        try {
            String data = objectMapper.writeValueAsString(customer);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, indexName, customer.getId()});
            client.prepareIndex(indexName, indexName)
                    .setId(customer.getId())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
        }

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        QueryStringQueryBuilder queryBuilder = QueryBuilders
                .queryString("ジャワ")
                .defaultField("firstName")
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        queryBuilder.analyzer("jpn_analyser");
        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());

    }

    @Test
    public void queryWithJapaneseAnalyserOnAll() throws InterruptedException {
        try {
            client.admin().indices().refresh(new RefreshRequest(indexName));
            final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(ImmutableSettings.settingsBuilder()
                            .loadFromSource(jsonBuilder()
                                    .startObject()
                                    .startObject("analysis")
                                            //
                                    .startObject("tokenizer")
                                    .startObject("kuromoji_user_dict")
                                    .field("type", "kuromoji_tokenizer")
                                    .field("mode", "search")
                                    .field("discard_punctuation", "false")
                                    .endObject()
                                    .endObject()
                                            //
                                    .startObject("analyzer")
                                            //named default
                                    .startObject("default")
                                    .field("type", "custom")
                                    .field("tokenizer", "kuromoji_user_dict")
                                    .endObject()
                                    .endObject()
                                            //
                                    .endObject()
                                    .endObject().string()));

            createIndexRequestBuilder.execute().actionGet();

        } catch (Exception e) {
            logger.error("Error creating index {}", indexName);
            logger.error(e.getMessage());
        }

        ObjectMapper objectMapper = new ObjectMapper();

        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "開発者ジャワ", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet);
        try {
            String data = objectMapper.writeValueAsString(customer);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, indexName, customer.getId()});
            client.prepareIndex(indexName, indexName)
                    .setId(customer.getId())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
        }

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        QueryStringQueryBuilder queryBuilder = QueryBuilders
                .queryString("ジャワ")
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        qb.minimumShouldMatch("1");
        queryBuilder.analyzer("default");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());

    }

    @Test
    public void query_WithNoAnalyserOnField() throws InterruptedException {
        try {
            client.admin().indices().refresh(new RefreshRequest(indexName));
            final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin()
                    .indices()
                    .prepareCreate(indexName);

            createIndexRequestBuilder.execute().actionGet();

        } catch (Exception e) {
            logger.error("Error creating index {}", indexName);
            logger.error(e.getMessage());
        }

        ObjectMapper objectMapper = new ObjectMapper();

        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "開発者ジャワ", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet);
        try {
            String data = objectMapper.writeValueAsString(customer);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, indexName, customer.getId()});
            client.prepareIndex(indexName, indexName)
                    .setId(customer.getId())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
        }

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        QueryStringQueryBuilder queryBuilder = QueryBuilders
                .queryString("ジャワ")
                .defaultField("firstName")
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());

    }
}
