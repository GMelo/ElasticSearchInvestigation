package org.gmelo.investigation.es.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.gmelo.investigation.es.indexing.IndexingService;
import org.gmelo.investigation.model.DateAsIntegerEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Set;

/**
 * Created by gmelo on 01/06/16.
 * ElasticSearchQueryServiceNumberQueryIT
 */
public class ElasticSearchQueryServiceNumberQueryIT {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;
    private ElasticSearchQueryService elasticSearchQueryService;
    private ElasticSearchService elasticSearchService;
    private IndexingService indexingService;

    @Before
    public void setUp() throws InterruptedException {
        client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));

        elasticSearchService = new ElasticSearchService(client);
        elasticSearchService.createIndex(ElasticSearchService.Index.date);
        elasticSearchQueryService = new ElasticSearchQueryService(client);
        indexingService = new IndexingService(client);
        Thread.sleep(500);
    }

    @Test
    public void index_WhenLocalDateGoE() throws InterruptedException {
        DateAsIntegerEntity dateAsIntegerEntity = new DateAsIntegerEntity("now", LocalDate.now());

        indexingService.index("date", dateAsIntegerEntity);
        String yesterday = LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        //  Integer yesterday = Integer.parseInt(LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE));

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.GREATER_OR_EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(1, response.getHits().totalHits());
        response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.LESS_OR_EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void index_WhenLocalDateLoE() throws InterruptedException {
        DateAsIntegerEntity dateAsIntegerEntity = new DateAsIntegerEntity("now", LocalDate.now());

        indexingService.index("date", dateAsIntegerEntity);
        String yesterday = LocalDate.now().plusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        //  Integer yesterday = Integer.parseInt(LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE));

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.LESS_OR_EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(1, response.getHits().totalHits());

        response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.GREATER_OR_EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void index_WhenLocalDateEquals() throws InterruptedException {
        DateAsIntegerEntity dateAsIntegerEntity = new DateAsIntegerEntity("now", LocalDate.now());

        indexingService.index("date", dateAsIntegerEntity);
        String yesterday = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        //  Integer yesterday = Integer.parseInt(LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE));

        Thread.sleep(900);
        SearchResponse response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(1, response.getHits().totalHits());

        response = elasticSearchQueryService.rangeQuery("localDateRepresentation", yesterday, ElasticSearchQueryService.RangeOperator.EQUAL, ElasticSearchService.Index.date);

        Assert.assertEquals(0, response.getHits().totalHits());
    }
}
