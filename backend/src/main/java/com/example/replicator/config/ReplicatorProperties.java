package com.example.replicator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties
public class ReplicatorProperties {
    private Db source = new Db();
    private Db sink = new Db();
    private Replication replication = new Replication();
    private Retry retry = new Retry();

    public Db getSource() {
        return source;
    }

    public void setSource(Db source) {
        this.source = source;
    }

    public Db getSink() {
        return sink;
    }

    public void setSink(Db sink) {
        this.sink = sink;
    }

    public Replication getReplication() {
        return replication;
    }

    public void setReplication(Replication replication) {
        this.replication = replication;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public static class Db {
        private String url;
        private String username;
        private String password;
        private String database;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }
    }

    public static class Replication {
        private long serverId = 987654L;
        private boolean autoStart = false;
        private boolean useGtid = true;
        private String snapshotMode = "initial";
        private boolean ddlEnabled = true;
        private boolean strictDdl = false;
        private boolean dmlEnabled = true;
        private List<String> includeTables = new ArrayList<>();
        private List<String> excludeTables = new ArrayList<>();
        private String onDuplicateInsert = "upsert";
        private boolean requirePrimaryKey = true;

        public long getServerId() {
            return serverId;
        }

        public void setServerId(long serverId) {
            this.serverId = serverId;
        }

        public boolean isAutoStart() {
            return autoStart;
        }

        public void setAutoStart(boolean autoStart) {
            this.autoStart = autoStart;
        }

        public boolean isUseGtid() {
            return useGtid;
        }

        public void setUseGtid(boolean useGtid) {
            this.useGtid = useGtid;
        }

        public String getSnapshotMode() {
            return snapshotMode;
        }

        public void setSnapshotMode(String snapshotMode) {
            this.snapshotMode = snapshotMode;
        }

        public boolean isDdlEnabled() {
            return ddlEnabled;
        }

        public void setDdlEnabled(boolean ddlEnabled) {
            this.ddlEnabled = ddlEnabled;
        }

        public boolean isStrictDdl() {
            return strictDdl;
        }

        public void setStrictDdl(boolean strictDdl) {
            this.strictDdl = strictDdl;
        }

        public boolean isDmlEnabled() {
            return dmlEnabled;
        }

        public void setDmlEnabled(boolean dmlEnabled) {
            this.dmlEnabled = dmlEnabled;
        }

        public List<String> getIncludeTables() {
            return includeTables;
        }

        public void setIncludeTables(List<String> includeTables) {
            this.includeTables = includeTables;
        }

        public List<String> getExcludeTables() {
            return excludeTables;
        }

        public void setExcludeTables(List<String> excludeTables) {
            this.excludeTables = excludeTables;
        }

        public String getOnDuplicateInsert() {
            return onDuplicateInsert;
        }

        public void setOnDuplicateInsert(String onDuplicateInsert) {
            this.onDuplicateInsert = onDuplicateInsert;
        }

        public boolean isRequirePrimaryKey() {
            return requirePrimaryKey;
        }

        public void setRequirePrimaryKey(boolean requirePrimaryKey) {
            this.requirePrimaryKey = requirePrimaryKey;
        }
    }

    public static class Retry {
        private int maxAttempts = 5;
        private long backoffMs = 3000;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getBackoffMs() {
            return backoffMs;
        }

        public void setBackoffMs(long backoffMs) {
            this.backoffMs = backoffMs;
        }
    }
}
