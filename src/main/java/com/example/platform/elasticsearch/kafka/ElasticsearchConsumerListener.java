package com.example.platform.elasticsearch.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.data.elasticsearch.kafka.KafkaEsModel;
import com.example.platform.elasticsearch.service.ElasticsearchConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * ElasticsearchConsumerListener.
 *
 * @author gao jx
 */
@Component
public class ElasticsearchConsumerListener {

    @Value("#{kafkaTopicProperties.elasticsearchSave}")
    private String saveTopic;

    @Value("#{kafkaTopicProperties.elasticsearchUpdate}")
    private String updateTopic;

    @Value("#{kafkaTopicProperties.elasticsearchDelete}")
    private String deleteTopic;

    private ElasticsearchConsumerService elasticsearchConsumerService;

    @Autowired
    public ElasticsearchConsumerListener(final ElasticsearchConsumerService elasticsearchConsumerServiceIn) {
        this.elasticsearchConsumerService = elasticsearchConsumerServiceIn;
    }

    /**
     * 监听kafka.
     *
     * @param record record
     * @param model  model
     */
    @KafkaListener(topics = {"#{kafkaTopicProperties.elasticsearchSave}", "#{kafkaTopicProperties.elasticsearchUpdate}", "#{kafkaTopicProperties.elasticsearchDelete}"})
    public void listener(final ConsumerRecord<String, Object> record, final KafkaEsModel model) {
        //获取监听的topic
        final String topic = record.topic();
        //判断topic
        if (saveTopic.equals(topic)) {
            elasticsearchConsumerService.save(model);
        }
        else if (updateTopic.equals(topic)) {
            elasticsearchConsumerService.update(model);
        }
        else if (deleteTopic.equals(topic)) {
            elasticsearchConsumerService.delete(model);
        }
    }

}
