package com.producer.indrivekafka.service;

import com.producer.indrivekafka.config.AppConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class KafkaService {

    private Logger logger= LoggerFactory.getLogger(KafkaService.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public boolean updateLocation(String location) {

            this.kafkaTemplate.send(AppConstants.LOCATION_TOPIC_NAME, location);
//            this.logger.info("message has been produced.........");
//            System.out.println(" hello man, its working");

        return true;
    }

}
