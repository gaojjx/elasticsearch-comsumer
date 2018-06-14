package com.example.platform.elasticsearch.repository;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.example.data.elasticsearch.kafka.KafkaEsModel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * ElasticsearchRepository.
 *
 * @author gao jx
 */
@Repository
public class ElasticsearchConsumerRepository {
    private static final String FAIL = "fail";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private RestHighLevelClient restHighLevelClient;

    @Autowired
    public ElasticsearchConsumerRepository(final RestHighLevelClient restHighLevelClientIn) {
        this.restHighLevelClient = restHighLevelClientIn;
    }

    /**
     * 保存ES.
     * @param model model
     * @return result
     */
    public String save(final KafkaEsModel model) {
        final String jsonValue = model.getJsonValue();
        try {
            final IndexRequest indexRequest = new IndexRequest(model.getIndex(), model.getType(), model.getId())
                    .source(jsonValue, XContentType.JSON);
            final IndexResponse indexResponse = restHighLevelClient.index(indexRequest);
            return indexResponse.getResult().name();
        }
        catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return FAIL;
        }
    }

    /**
     * 更新ES.
     * @param model model
     * @return result
     */
    public String update(final KafkaEsModel model) {
        final String jsonValue = model.getJsonValue();
        final UpdateRequest request = new UpdateRequest(model.getIndex(), model.getType(), model.getId());
        request.doc(jsonValue, XContentType.JSON);
        try {
            final UpdateResponse response = restHighLevelClient.update(request);
            return response.getResult().getLowercase();
        }
        catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return FAIL;
        }
    }

    /**
     * 删除ES.
     * @param model model
     * @return result
     */
    public String delete(final KafkaEsModel model) {
        final DeleteRequest request = new DeleteRequest(model.getIndex(), model.getType(), model.getId());
        try {
            final DeleteResponse response = restHighLevelClient.delete(request);
            return response.getResult().getLowercase();
        }
        catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return FAIL;
        }
    }

}
