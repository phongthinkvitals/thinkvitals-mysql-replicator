package com.example.replicator.web;

import com.example.replicator.model.ReplicationStatus;
import com.example.replicator.model.ReplicationVerificationSummary;
import com.example.replicator.model.TableVerificationStatus;
import com.example.replicator.service.ReplicationService;
import com.example.replicator.service.VerificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Map;

@RestController
public class MonitoringController {
    private final ReplicationService replicationService;
    private final VerificationService verificationService;

    public MonitoringController(ReplicationService replicationService,
                                VerificationService verificationService) {
        this.replicationService = replicationService;
        this.verificationService = verificationService;
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/auth/me")
    Map<String, String> me(Principal principal) {
        return Map.of("username", principal.getName());
    }

    @GetMapping("/replication/status")
    ReplicationStatus status() {
        return replicationService.status();
    }

    @GetMapping("/replication/verify")
    TableVerificationStatus verify(@RequestParam String table,
                                   @RequestParam(required = false) Integer limit) {
        return verificationService.verifyTable(table, limit);
    }

    @GetMapping("/replication/verify/all")
    ReplicationVerificationSummary verifyAll(@RequestParam(required = false) Integer limit) {
        return verificationService.verifyAll(limit);
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
