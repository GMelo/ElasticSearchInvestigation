package org.gmelo.investigation.es.creation;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.gmelo.investigation.model.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchService {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchService.class);

    public enum Index {customer, product}

    private final Client client;
    public final static String JAPANESE_LANGUAGE_ANALYSIS = "japanese_analyzer";

    public ElasticSearchService(Client client) {
        this.client = client;
    }


    public ElasticSearchService(String server, int port, String clusterName) {
        logger.debug("Creating ES Service Instance on:{} and port:{}", server, port);
        this.client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(server, port));
    }

    public void createIndex(Index index) {
        switch (index) {
            case customer:
                createCustomerIndex();
                break;
            case product:
                break;
            default:
                logger.error("Unknown index:{}", index);
        }
    }

    /**
     * Creates a Index for the customer using a japanese language tokenizer
     *
     */
    private void createCustomerIndex() {
        try {
            client.admin().indices().refresh(new RefreshRequest(Index.customer.name()));
            final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin()
                    .indices()
                    .prepareCreate(Index.customer.name())
                    .setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
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
                            .startObject(JAPANESE_LANGUAGE_ANALYSIS)
                            .field("type", "custom")
                            .field("tokenizer", "kuromoji_user_dict")
                            .endObject()
                            .endObject()
                                    //
                            .endObject()
                            .endObject().string()));

            createIndexRequestBuilder.execute().actionGet();

            String indexProperties = Customer.indexProperties();

            //if a index had properties
            if (indexProperties != null) {
                createIndexProperties(Index.customer.name(), indexProperties);
            }

        } catch (IOException e) {
            logger.error("Error Customer company Index");
            logger.error(e.getMessage());
        }
    }


    /**
     * @param indexName the name of the index
     * @param properties the specific properties of the index
     */
    private void createIndexProperties(String indexName, String properties) {
        client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setType(indexName)
                .setSource(properties)
                .execute()
                .actionGet();
    }

    /**
     * Deletes a index by name
     *
     * @param indexName the index to be deleted
     */
    public void deleteIndex(Index indexName) {
        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName.name())).actionGet();

            logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
