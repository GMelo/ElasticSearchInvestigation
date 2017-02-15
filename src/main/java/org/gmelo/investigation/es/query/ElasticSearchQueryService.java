package org.gmelo.investigation.es.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.gmelo.investigation.es.creation.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gmelo on 23/09/15.
 */
public class ElasticSearchQueryService {

    private final Client client;
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchQueryService.class);

    public enum RangeOperator {GREATER_OR_EQUAL, LESS_OR_EQUAL, EQUAL}

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
        //   queryBuilder.analyzer("default");
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

    public SearchResponse rangeQuery(String field, Object value, RangeOperator operator, ElasticSearchService.Index index) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        RangeQueryBuilder rangeQuery;
        switch (operator) {
            case GREATER_OR_EQUAL:
                rangeQuery = QueryBuilders
                        .rangeQuery(field)
                        .gte(value);
                break;
            case LESS_OR_EQUAL:
                rangeQuery = QueryBuilders
                        .rangeQuery(field)
                        .lte(value);
                break;
            default:
                rangeQuery = QueryBuilders
                        .rangeQuery(field)
                        .gte(value)
                        .lte(value);
                break;
        }
        qb.must(rangeQuery);

        searchRequestBuilder.setQuery(qb);

        return searchRequestBuilder
                .setFrom(1)
                .setSize(10)
                .setIndices(index.name())
                .execute()
                .actionGet();
    }

}
