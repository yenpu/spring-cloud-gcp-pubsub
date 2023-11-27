package com.example.gcp.pubsub;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pubsub")
public record PubSubConfig(String topic, String subscription) {
}
