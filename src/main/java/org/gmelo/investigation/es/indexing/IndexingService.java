package org.gmelo.investigation.es.indexing;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;
import org.gmelo.investigation.model.Customer;
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
}
