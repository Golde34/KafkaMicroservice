package com.example.kafkamicroservice;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.List;

@ConfigurationPropertiesScan
@ConfigurationProperties(prefix = "spring.kafka")
public class GetListServer {
    private List<ServerEntity> servers;

    public List<ServerEntity> getServers() {
        return servers;
    }

    public void setServers(List<ServerEntity> servers) {
        this.servers = servers;
    }


}
