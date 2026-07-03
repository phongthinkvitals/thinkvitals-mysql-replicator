package com.example.replicator;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class MonitoringController {
    private final ReplicationService replicationService;

    public MonitoringController(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/replication/status")
    ReplicationStatus status() {
        return replicationService.status();
    }

    @PostMapping("/replication/pause")
    Map<String, String> pause() {
        replicationService.pause();
        return Map.of("status", "PAUSED");
    }

    @PostMapping("/replication/resume")
    Map<String, String> resume() {
        replicationService.resume();
        return Map.of("status", "RUNNING");
    }
}
