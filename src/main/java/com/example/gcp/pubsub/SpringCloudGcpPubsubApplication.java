package com.example.gcp.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SpringCloudGcpPubsubApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringCloudGcpPubsubApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudGcpPubsubApplication.class, args);
    }

    // Create an inbound channel adapter to listen to the subscription and send
    // messages to the input message channel.
    @Bean
    public PubSubInboundChannelAdapter inboundChannelAdapter(
            @Qualifier("inputMessageChannel") final MessageChannel messageChannel,
            final PubSubTemplate pubSubTemplate,
            final PubSubConfig config) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, config.subscription());
        adapter.setOutputChannel(messageChannel);
        adapter.setAckMode(AckMode.MANUAL);
        adapter.setPayloadType(String.class);
        return adapter;
    }

    // Create a message channel for messages arriving from the subscription.
    @Bean
    public MessageChannel inputMessageChannel() {
        return new DirectChannel();
    }

    private PubsubOutboundGateway gateway;
    private ObjectMapper mapper;

    // Define what happens to the messages arriving in the message channel.
    @ServiceActivator(inputChannel = "inputMessageChannel")
    public void messageReceiver(
            final String payload,
            @Header(GcpPubSubHeaders.ORIGINAL_MESSAGE) final BasicAcknowledgeablePubsubMessage message) {
        LOGGER.info("Message arrived via an inbound channel adapter from apps-to-uys-subscription! Payload: " + payload);
        message.ack();

        if (gateway == null) {
            this.gateway = ApplicationContextProvider.getApplicationContext().getBean(PubsubOutboundGateway.class);
        }
        if (mapper == null) {
            this.mapper = new ObjectMapper();
        }

        try {
            final ObjectNode node = this.mapper.readValue(payload, ObjectNode.class);
            if ("delay".equals(node.get("cmd").asText())) {
                TimeUnit.MILLISECONDS.sleep(100);
                LOGGER.info("Message delayed");
            }
            node.put("state", UUID.randomUUID().toString());
            this.gateway.sendToPubsub(node.toString());
        } catch (final Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
    public interface PubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    @Bean
    @ServiceActivator(inputChannel = "pubsubOutputChannel")
    public MessageHandler messageSender(final PubSubTemplate pubsubTemplate, final PubSubConfig config) {
        PubSubMessageHandler adapter = new PubSubMessageHandler(pubsubTemplate, config.topic());

        adapter.setSuccessCallback(
                ((ackId, message) ->
                        LOGGER.info("Message was sent via the outbound channel adapter to uys-to-apps-topic! " + message + ", ackId: " + ackId)));

        adapter.setFailureCallback(
                (cause, message) -> LOGGER.info("Error sending " + message + " due to " + cause));

        return adapter;
    }

}
