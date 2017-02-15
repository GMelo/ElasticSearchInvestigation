package org.gmelo.investigation.es.indexing;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.gmelo.investigation.es.query.ElasticSearchQueryService;
import org.gmelo.investigation.model.Address;
import org.gmelo.investigation.model.Customer;
import org.gmelo.investigation.model.Qualification;
import org.gmelo.investigation.model.Telephone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Set;

public class IndexingServiceTestIT {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;
    private ElasticSearchQueryService elasticSearchQueryService;
    private ElasticSearchService elasticSearchService;
    private IndexingService indexingService;

    @Before
    public void setUp() {
        client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));

        elasticSearchService = new ElasticSearchService(client);
        elasticSearchService.createIndex(ElasticSearchService.Index.customer);
        elasticSearchQueryService = new ElasticSearchQueryService(client);
        indexingService = new IndexingService(client);
    }

    @After
    public void tearDown() {
        elasticSearchService.deleteIndex(ElasticSearchService.Index.customer);
    }

    @Test
    public void index_WhenSimpleAdd() throws InterruptedException {
        Set<String> interestSet = null;
        Set<Address> addressSet = null;
        Set<Telephone> telephoneSet = null;
        Integer numberOfCalls = 20;
        Customer customer = new Customer("1", "Guilherme", "Melo", "title", "Test Writer", "test@gmail.org", interestSet, addressSet, telephoneSet, numberOfCalls, null, LocalDate.now());

        Qualification qualification = new Qualification();
        qualification.setDate(LocalDate.now());
        qualification.setInstitution("LaSalle");
        qualification.setType(Qualification.Type.ACADEMIC);
        qualification.setField("IT");
        customer.addQualification(qualification);

        indexingService.index("customer", customer);

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.queryForFieldAndTerm("_all", "Guilherme", ElasticSearchService.Index.customer);

        Assert.assertEquals(1, response.getHits().totalHits());
    }

}