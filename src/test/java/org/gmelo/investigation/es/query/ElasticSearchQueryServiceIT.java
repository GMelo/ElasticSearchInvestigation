package org.gmelo.investigation.es.query;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.gmelo.investigation.es.indexing.IndexingService;
import org.gmelo.investigation.model.Address;
import org.gmelo.investigation.model.Customer;
import org.gmelo.investigation.model.Telephone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ElasticSearchQueryServiceIT {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;
    private ElasticSearchQueryService elasticSearchQueryService;
    private ElasticSearchService elasticSearchService;
    private IndexingService indexingService;
    private Integer numberOfCalls = 0;

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

    @After
    public void tearDown() {
        elasticSearchService.deleteIndex(ElasticSearchService.Index.customer);
    }

    @Test
    public void index_WhenAll() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "Guilherme", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenAllLong() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "輸出貿易事務9年、メーカーの営業アシスタン", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "輸出貿易事務9年、メーカーの営業アシスタン", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenAllLongNotAllMatch() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "輸出貿易事務9年、メーカーの営業アシスタン", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "メーカーの営業アシスタン", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenAll_JPNWildcard() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "輸出貿易", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "*", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }


    @Test
    public void index_WhenAll_JPN() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "輸出貿易", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "輸出", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenDirectField_JPN() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "輸出貿易", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("firstName", "輸出", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenDirectField_JPNField() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "ント", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("firstName", "ント", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenDirectField() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("firstName", "Guilherme", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenDirectFieldWildcard() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("firstName", "Gui*", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenListOfString() throws InterruptedException {
        Set<String> interestSet = new HashSet<String>(Arrays.asList("Java", "Spring", "Elastic Search"));
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "Java", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenListOfStringField() throws InterruptedException {
        Set<String> interestSet = new HashSet<String>(Arrays.asList("Java", "Spring", "Elastic Search"));
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);
        indexingService.index("customer", customer);
        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("interestSet", "Java", ElasticSearchService.Index.customer);
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenListOfComplexField() throws InterruptedException {
        Set<String> interestSet = new HashSet<String>(Arrays.asList("Java", "Spring", "Elastic Search"));
        //    public Address(String number, String streetName, String complement, String city, String country) {
        Set<Address> addressSet = new HashSet<Address>();
        addressSet.add(new Address("302", "AV. 4", "Maravista", "Rio de Janeiro", "Brazil"));
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);
        indexingService.index("customer", customer);
        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("addressSet", "Brazil", ElasticSearchService.Index.customer);
        Assert.assertEquals(0, response.getHits().totalHits());

        response = elasticSearchQueryService.queryForFieldAndTerm("addressSet.country", "Brazil", ElasticSearchService.Index.customer);
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void index_WhenListOfComplexFieldOnAll() throws InterruptedException {
        Set<String> interestSet = new HashSet<String>(Arrays.asList("Java", "Spring", "Elastic Search"));
        //    public Address(String number, String streetName, String complement, String city, String country) {
        Set<Address> addressSet = new HashSet<Address>();
        addressSet.add(new Address("302", "AV. 4", "Maravista", "Rio de Janeiro", "Brazil"));
        Set<Telephone> telephoneSet = null;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls);
        indexingService.index("customer", customer);
        Thread.sleep(900);

        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "Brazil", ElasticSearchService.Index.customer);
        Assert.assertEquals(1, response.getHits().totalHits());
    }
}