package org.gmelo.investigation.es.creation;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ElasticSearchServiceIT {

    private final String server = "127.0.0.1";
    private final int port = 9300;
    private final String clusterName = "gmelo-local-elasticsearch";

    private Client client;

    @Before
    public void setUp() {
        client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));
    }

    @Test
    public void creation_WhenTokenizerInitialized() {
        ElasticSearchService elasticSearchService = new ElasticSearchService(client);
        elasticSearchService.createIndex(ElasticSearchService.Index.customer);

        Assert.assertTrue(client.admin()
                .indices()
                .exists(new IndicesExistsRequest(ElasticSearchService.Index.customer.name()))
                .actionGet().isExists());

        elasticSearchService.deleteIndex(ElasticSearchService.Index.customer);

        Assert.assertFalse(client.admin()
                .indices()
                .exists(new IndicesExistsRequest(ElasticSearchService.Index.customer.name()))
                .actionGet().isExists());

    }

}