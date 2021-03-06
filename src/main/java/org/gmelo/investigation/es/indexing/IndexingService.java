package org.gmelo.investigation.es.indexing;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;
import org.gmelo.investigation.model.Customer;
import org.gmelo.investigation.model.DateAsIntegerEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by gmelo on 22/09/15.
 * Will add a element o a ES collection
 */
public class IndexingService {

    private static Logger logger = LoggerFactory.getLogger(IndexingService.class);

    private final Client client;

    public IndexingService(Client client) {
        this.client = client;
    }

    public void index(String indexName, Customer customer) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String data = objectMapper.writeValueAsString(customer);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, indexName, customer.getId()});
            client.prepareIndex(indexName, indexName)
                    .setId(customer.getId())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
            e.printStackTrace();
        }
    }

    public void index(String indexName, DateAsIntegerEntity entity) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String data = objectMapper.writeValueAsString(entity);

            logger.debug("Sending index indexName={} indexType={} id={}", new Object[]{indexName, indexName, entity.getName()});
            client.prepareIndex(indexName, indexName)
                    .setId(entity.getName())
                    .setSource(data)
                    .execute().actionGet();

        } catch (IOException e) {
            logger.error("Error sending Index {}", e);
            e.printStackTrace();
        }
    }
}
