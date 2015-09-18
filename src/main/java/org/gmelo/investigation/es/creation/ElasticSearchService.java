package org.gmelo.investigation.es.creation;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.codehaus.jackson.map.ObjectMapper;
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

            if (indexProperties != null) {
                createIndexProperties(Index.customer.name(), indexProperties);
            }

        } catch (IOException e) {
            logger.error("Error Customer company Index");
            logger.error(e.getMessage());
        }
    }

    public void indexCandidate(String indexName, String type, Customer customer) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String data = objectMapper.writeValueAsString(customer);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, type, customer.getId()});
            client.prepareIndex(indexName, type)
                    .setId(customer.getId())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
            e.printStackTrace();
        }
    }

    private void createIndexProperties(String indexName, String properties) {
        client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setType(indexName)
                .setSource(properties)
                .execute()
                .actionGet();
    }

    public void deleteIndex(String indexName) {
        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();

            logger.debug("Delete index response={}", ToStringBuilder.reflectionToString(deleteIndexResponse));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
