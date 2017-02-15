package org.gmelo.investigation.es.query;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.gmelo.investigation.es.indexing.IndexingService;
import org.gmelo.investigation.model.Address;
import org.gmelo.investigation.model.Customer;
import org.gmelo.investigation.model.Telephone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;

/**
 * Created by gmelo on 26/10/15.
 */
public class NumericQuery {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;
    private ElasticSearchQueryService elasticSearchQueryService;
    private ElasticSearchService elasticSearchService;
    private IndexingService indexingService;
    private LocalDateTime localDateTime = null;
    private LocalDate localDate = null;

    @Before
    public void setUp() throws InterruptedException {
        client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));


        elasticSearchService = new ElasticSearchService(client);
        elasticSearchService.createIndex(ElasticSearchService.Index.customer);
        elasticSearchQueryService = new ElasticSearchQueryService(client);
        indexingService = new IndexingService(client);
        Thread.sleep(500);
    }

    @Test
    public void simpleNumericQuery_WhenGreaterThan20() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 30;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, null, null);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .gt(20);


        //QueryStringQueryBuilder queryBuilder = QueryBuilders

//                .queryString(term)
//                .defaultField(field)
        //  .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        //   queryBuilder.analyzer("default");
        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void simpleNumericQuery_WhenFewerThan50() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 30;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, null, null);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50);


        //QueryStringQueryBuilder queryBuilder = QueryBuilders

//                .queryString(term)
//                .defaultField(field)
        //  .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        //   queryBuilder.analyzer("default");
        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void simpleNumericQuery_WhenFewerThanEqual50() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 50;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, null);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50);


        //QueryStringQueryBuilder queryBuilder = QueryBuilders

//                .queryString(term)
//                .defaultField(field)
        //  .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        //   queryBuilder.analyzer("default");
        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }


    @Test
    public void simpleNumericQuery_WhenFewerThanEqual50AndNameMatch() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 50;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, null);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50);


        QueryStringQueryBuilder stringQueryBuilder = QueryBuilders
                .queryString("Guilherme")
                .defaultField("firstName")
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);

        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);
        qb.must(stringQueryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void simpleNumericQuery_NoMatchWhenFewerThanEqual50AndNameMatch() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 50;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, null);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50);


        QueryStringQueryBuilder stringQueryBuilder = QueryBuilders
                .queryString("not_match")
                .defaultField("firstName")
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);

        qb.minimumShouldMatch("1");

        qb.must(queryBuilder);
        qb.must(stringQueryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void simpleNumericQuery_WhenFewerThan50AndMoreThan25() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 30;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, localDate);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50)
                .gte(25);

        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }


    @Test
    public void simpleNumericQuery_WhenFewerThan50AndMoreThan25SeparateQuery() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 40;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, localDate);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder less = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(50);

        RangeQueryBuilder greater = QueryBuilders
                .rangeQuery("numberOfCalls")
                .gte(25);

        qb.minimumShouldMatch("1");
        qb.must(less);
        qb.must(greater);
        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(1, response.getHits().totalHits());
    }
    @Test
    public void simpleNumericQuery_WhenFalse() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = null;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, numberOfCalls, localDateTime, localDate);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(40);


        //QueryStringQueryBuilder queryBuilder = QueryBuilders

//                .queryString(term)
//                .defaultField(field)
        //  .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        //   queryBuilder.analyzer("default");

        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void simpleNumericQuery_WhenFNull() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = null;
        String indexName = "customer";
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org",
                interestSet, addressSet, telephoneSet, null, localDateTime, localDate);

        indexingService.index(indexName, customer);

        Thread.sleep(900);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder queryBuilder = QueryBuilders
                .rangeQuery("numberOfCalls")
                .lte(40)
                .gte(0);


        //QueryStringQueryBuilder queryBuilder = QueryBuilders

//                .queryString(term)
//                .defaultField(field)
        //  .defaultOperator(QueryStringQueryBuilder.Operator.AND);
        //   queryBuilder.analyzer("default");

        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        SearchResponse response = searchRequestBuilder
                .setIndices(indexName)
                .execute()
                .actionGet();


        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            //   logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            //  logger.error(e.getMessage());
        }

        Assert.assertEquals(0, response.getHits().totalHits());
    }

}
