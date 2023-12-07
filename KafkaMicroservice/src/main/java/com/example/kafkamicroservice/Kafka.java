package com.example.kafkamicroservice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Component
@EnableConfigurationProperties(GetListServer.class)
public class Kafka {
    Logger logger = Logger.getLogger(Kafka.class.getName());
    @Autowired
    private GetListServer getListServer;

    @Bean
    private boolean loadKafkaServerConfig() throws Exception {
        List<ServerEntity> configs = getListServer.getServers();
        ExecutorService executorService = Executors.newCachedThreadPool();

        for (ServerEntity config : configs) {
            executorService.submit(() -> {
                logger.info("bootstrapServers: " + config.getBootstrapServers());
                logger.info("groupId: " + config.getGroupId());
                try {
                    messageListener(config.getBootstrapServers(), config.getGroupId());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        executorService.shutdown(); // Đảm bảo shutdown executor service khi không cần nữa
        return true;
    }

    public boolean messageListener(String bs, String groupId) throws Exception {
        logger.info("Start auto");

        AcknowlegingMessageListener<String, String> messageListener = (record, acknowlegement) -> {
            String message = record.value();
            logger.info("Received message: " + message);
            int partition = record.partition();
            logger.info("From partition: " + partition);
            long offset = record.offset();
            logger.info("From offset: " + offset);
            String topic = record.topic();
            logger.info("Received from topic: " + topic);

            acknowlegement.acknowledge();
        }

        ContainerProperties containerProps = new ContainerProperties("topic1");
        containerProps.setGroupId(groupId);
        // Set the message listener in container properties
        containerProps.setMessageListener(messageListener);
        containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
        containerProps.setPollTimeout(3000);
        containerProps.setSyncCommits(true);

        KafkaMessageListenerContainer<Integer, String> container = createContainer(bs, groupId, containerProps);

        container.setBeanName("testAuto");
        container.start();

        return true;
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(String bootstrapServers, String groupId, ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps(bootstrapServers, groupId);
        DefaultKafkaConsumerFactory<Integer, String> cf =
                new DefaultKafkaConsumerFactory<Integer, String>(props);
        return new KafkaMessageListenerContainer<>(cf, containerProps);
    }

    private KafkaTemplate<Integer, String> createTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<Integer, String> pf =
                new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        return template;
    }

    private Map<String, Object> consumerProps(String bootstrapServers, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
