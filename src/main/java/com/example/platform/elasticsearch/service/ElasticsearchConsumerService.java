package com.example.platform.elasticsearch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.data.elasticsearch.kafka.KafkaEsModel;
import com.example.platform.elasticsearch.repository.ElasticsearchConsumerRepository;

/**
 * ElasticsearchService.
 *
 * @author gao jx
 */
@Service
public class ElasticsearchConsumerService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String fail = "fail";

    private RestTemplate restTemplate;

    private ElasticsearchConsumerRepository elasticsearchConsumerRepository;

    @Autowired
    public ElasticsearchConsumerService(final RestTemplate restTemplateIn,
                                        final ElasticsearchConsumerRepository elasticsearchConsumerRepositoryIn) {
        this.restTemplate = restTemplateIn;
        this.elasticsearchConsumerRepository = elasticsearchConsumerRepositoryIn;
    }

    /**
     * 保存ES.
     *
     * @param model model
     */
    public void save(final KafkaEsModel model) {
        final String uri = model.getUri();
        final String jsonValue = restTemplate.getForObject(uri, String.class);
        if (jsonValue == null || fail.equals(jsonValue)) {
            logger.error("获取URI:{}失败", model.getUri());
        }
        else {
            model.setJsonValue(jsonValue);
            final String result = elasticsearchConsumerRepository.save(model);
            logger.debug("es index:{}, type:{}, id:{}, 保存结果:{}", model.getIndex(), model.getType(), model.getId(), result);
        }
    }

    /**
     * 更新ES.
     *
     * @param model model
     */
    public void update(final KafkaEsModel model) {
        final String uri = model.getUri();
        final String jsonValue = restTemplate.getForObject(uri, String.class);
        if (jsonValue == null || fail.equals(jsonValue)) {
            logger.error("获取URI:{}失败", model.getUri());
        }
        else {
            model.setJsonValue(jsonValue);
            final String result = elasticsearchConsumerRepository.update(model);
            logger.debug("es index:{}, type:{}, id:{}, 更新结果:{}", model.getIndex(), model.getType(), model.getId(), result);
        }
    }

    /**
     * 删除ES.
     *
     * @param model model
     */
    public void delete(final KafkaEsModel model) {
        final String result = elasticsearchConsumerRepository.delete(model);
        logger.debug("es index:{}, type:{}, id:{}, 删除结果:{}", model.getIndex(), model.getType(), model.getId(), result);
    }
}
