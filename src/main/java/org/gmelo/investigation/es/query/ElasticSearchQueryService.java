package org.gmelo.investigation.es.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gmelo on 23/09/15.
 */
public class ElasticSearchQueryService {

    private final Client client;
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchQueryService.class);

    public ElasticSearchQueryService(Client client) {
        this.client = client;
    }

    public SearchResponse queryForFieldAndTerm(String field, String term, ElasticSearchService.Index index) {
        logger.info("Searching index {} for term: {}", index.name(), term);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        QueryStringQueryBuilder queryBuilder = QueryBuilders
                .queryString(term)
                .defaultField(field)
                .defaultOperator(QueryStringQueryBuilder.Operator.AND);

        qb.minimumShouldMatch("1");
        qb.must(queryBuilder);

        searchRequestBuilder.setQuery(qb);

        return searchRequestBuilder
                .setFrom(1)
                .setSize(10)
                .setIndices(index.name())
                .execute()
                .actionGet();
    }
}
