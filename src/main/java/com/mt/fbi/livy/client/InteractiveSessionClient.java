package com.mt.fbi.livy.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mt.fbi.livy.util.Helper;
import com.mt.fbi.livy.util.JsonUtil;
import com.mt.fbi.livy.util.OkHttpRestClient;
import com.mt.fbi.livy.util.RestClient;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by yihaibo on 2019-04-18.
 */
@Slf4j
public class InteractiveSessionClient implements AutoCloseable {
    private String livyURL;
    private Map<String, Object> livyConf;
    private boolean restartDeadSession;
    private RestClient restClient;
    private int currentSessionId;
    private volatile SessionInfo currentSessionInfo;

    public static Builder builder() {
        return new Builder();
    }

    public InteractiveSessionClient(String livyURL, Map<String, Object> livyConf, boolean restartDeadSession,
                                    RestClient restClient, int currentSessionId) {
        this.livyURL = livyURL;
        this.livyConf = livyConf;
        this.restartDeadSession = restartDeadSession;
        this.restClient = restClient;
        this.currentSessionId = currentSessionId;
    }

    public SessionInfo getCurrentSessionInfo() {
        return this.currentSessionInfo;
    }

    /**
     * 阻塞创建session，直到session状态可用
     */
    public void awaitOpen() throws LivyException {
        if(currentSessionId > -1) {
            this.currentSessionInfo = getSessionInfo(currentSessionId);
            log.info("Get livy session successfully with sessionId: {}", currentSessionId);
        }else {
            this.currentSessionInfo = awaitInitSession();
            log.info("Create livy session successfully with sessionId: {}", this.currentSessionInfo.id);
        }
    }

    /**
     * @param code  执行代码
     * @param durationMillis  等待间隔
     * @param handleStatementInfo  处理函数
     * @return
     * @throws LivyException
     */
    public StatementInfo awaitExecuteStatement(String code, long durationMillis, Consumer<StatementInfo> handleStatementInfo) throws LivyException {
        StatementInfo stmtInfo = executeStatement(code);
        try{
            for(;;) {
                if(stmtInfo.isAvailable()) {
                    break;
                }
                Helper.sleep(durationMillis);
                stmtInfo = getStatementInfo(stmtInfo.id);
                if(Objects.nonNull(handleStatementInfo)) {
                    handleStatementInfo.accept(stmtInfo);
                }
            }
        }catch (Exception e) {
            log.error("awaitExecuteStatement, code:{}, e:", code, e);
            throw new LivyException(e);
        }

        if(stmtInfo.getOutput().isError()) {
            throw new LivyException("failed execute code:" + stmtInfo.getOutput().getEvalue());
        }
        return stmtInfo;
    }

    /**
     * 阻塞执行code，直到返回结果
     */
    public StatementInfo awaitExecuteStatement(String code) throws LivyException {
        return awaitExecuteStatement(code,1000L, null);
    }

    public SessionInfo createSessionInfo() throws IOException {
        return SessionInfo.fromJson(this.restClient.post(this.livyURL + "/sessions", this.livyConf));
    }

    public SessionInfo getSessionInfo() throws IOException {
        return SessionInfo.fromJson(this.restClient.get(this.livyURL + "/sessions/" + this.currentSessionInfo.getId()));
    }

    /**
     * Livy server对于过期session的定义为当前时间减去最后一次活跃时间的差值大于设置的阈值(现在设置的值为30min)时，判定为过期，被gc线程回收。
     *
     * 解决方案就是更新session的最后一次活跃时间
     */
    public void connect(){
        try{
            this.restClient.post(this.livyURL + "/sessions/" + this.currentSessionInfo.getId() + "/connect", "");
        }catch (Exception e) {
            log.error("connect error, sessionId:{}, e:", this.currentSessionInfo.getId(), e);
        }
    }

    public void ping() {
        this.connect();
    }

    public SessionInfo getSessionInfo(int sessionId) throws LivyException {
        try {
            return SessionInfo.fromJson(this.restClient.get(this.livyURL + "/sessions/" + sessionId));
        }catch (Exception e) {
            log.error("getSessionInfo error, sessionId:{}, e:", sessionId, e);
            throw new LivyException("get livy session error:" + sessionId);
        }
    }

    public SessionLog getSessionLog() throws IOException {
        return SessionLog.fromJson(this.restClient.get(this.livyURL + "/sessions/" + this.currentSessionInfo.getId() + "/log?size=" + 1000));
    }

    @Override
    public void close() {
        if(Objects.nonNull(this.currentSessionInfo)) {
            this.closeSession();
            this.currentSessionInfo = null;
        }
    }

    public StatementInfo executeStatement(String code) throws LivyException {
        StatementInfo stmtInfo = null;
        try {
            StatementRequest request = new StatementRequest(code, this.currentSessionInfo.kind.getKind());
            try {
                stmtInfo = this.executeStatement(request);
                return stmtInfo;
            }catch (Exception e) {
                SessionInfo sessionInfo = getSessionInfo();
                if (sessionInfo.isFinished()) {
                    this.close();
                    if (!this.restartDeadSession) {
                        log.info("interpret session finished, sessionId:{}, state:{}", sessionInfo.getId(), sessionInfo.getState().getState());
                        throw new LivyException("session finished state:" + sessionInfo.getState().getState());
                    }
                    synchronized (this) {
                        this.awaitOpen();
                    }
                }
                stmtInfo = this.executeStatement(request);
                return stmtInfo;
            }
        }catch (Exception e) {
            log.error("executeStatement error, sessionId:{}, code:{}, e:", this.currentSessionInfo.getId(), code, e);
            throw new LivyException(e);
        }
    }

    public StatementInfo getStatementInfo(int statementId) throws IOException {
        return StatementInfo.fromJson(this.restClient.get(this.livyURL + "/sessions/" + this.currentSessionInfo.getId() + "/statements/" + statementId));
    }

    public void cancelStatement(int statementId) throws IOException {
        this.restClient.post(this.livyURL + "/sessions/" + this.currentSessionInfo.getId() + "/statements/" + statementId + "/cancel", "");
    }

    private StatementInfo executeStatement(StatementRequest executeRequest) throws IOException {
        return StatementInfo.fromJson(this.restClient.post(this.livyURL + "/sessions/" + this.currentSessionInfo.getId() + "/statements", executeRequest));
    }

    private SessionInfo awaitInitSession() throws LivyException {
        try {
            this.currentSessionInfo = createSessionInfo();
            log.info("Session is creating");
            String msg;
            do{
                if(this.currentSessionInfo.isReady()) {
                    return this.currentSessionInfo;
                }
                Thread.sleep(1000L);
                this.currentSessionInfo = this.getSessionInfo();
                if (this.currentSessionInfo.isReady()) {
                    log.info("Session {} has been established.", this.currentSessionInfo.id);
                    log.info("Application Id: {}, Tracking URL: {}", this.currentSessionInfo.appId, this.currentSessionInfo.appInfo.get("sparkUiUrl"));
                } else {
                    log.info("Session {} is in state {}, appId {}", this.currentSessionInfo.id, this.currentSessionInfo.state, this.currentSessionInfo.appId);
                }
                if(this.currentSessionInfo.isFinished()) {
                    msg = "Create Session " + this.currentSessionInfo.id + " is finished, appId: " + this.currentSessionInfo.appId + ", log:\n" +
                            this.getSessionLog().logs();
                    throw new LivyException(msg);
                }
            } while(!this.currentSessionInfo.isFinished());

            msg = "Session " + this.currentSessionInfo.id + " start error, log:\n" + this.getSessionLog().logs();
            throw new LivyException(msg);
        }catch (Exception e) {
            log.error("Error when creating livy session", e);
            this.currentSessionInfo = null;
            throw new LivyException(e);
        }
    }

    private void closeSession() {
        try {
            this.restClient.delete(this.livyURL +"/sessions/" + this.currentSessionInfo.getId());
        } catch (Exception e) {
            log.error("Error closing session for user with session ID: {}, e:", this.currentSessionInfo.getId(), e);
        }
    }

    public static class Builder {
        private String livyURL;
        private int currentSessionId = -1;
        private Map<String, Object> livyConf = new HashMap<>();
        private boolean restartDeadSession = true;
        private RestClient restClient = new OkHttpRestClient();

        Builder() {
        }

        public Builder livyURL(String livyURL) {
            this.livyURL = livyURL;
            return this;
        }

        public Builder currentSessionId(int currentSessionId) {
            this.currentSessionId = currentSessionId;
            return this;
        }

        public Builder livyConf(Map<String, Object> livyConf) {
            this.livyConf.putAll(livyConf);
            return this;
        }

        public Builder restartDeadSession(boolean restartDeadSession) {
            this.restartDeadSession = restartDeadSession;
            return this;
        }

        public Builder restClient(RestClient restClient) {
            this.restClient = restClient;
            return this;
        }

        /**
         * @param kind The session kind http://livy.incubator.apache.org/docs/latest/rest-api.html#session-kind
         */
        public Builder kind(SessionKind kind) {
            livyConf.put("kind", kind.getKind());
            return this;
        }


        /**
         * @param name The name of this session
         */
        public Builder name(String name) {
            livyConf.put("name", name);
            return this;
        }

        public InteractiveSessionClient build() {
            if(StringUtils.isBlank(livyURL) && livyConf.containsKey("livyURL")) {
                livyURL = livyConf.get("livyURL").toString();
            }

            if(StringUtils.isBlank(livyURL)) {
                throw new IllegalArgumentException("miss livyURL");
            }
            if(!livyConf.containsKey("kind")) {
                throw new IllegalArgumentException("livy conf miss kind");
            }
            InteractiveSessionClient client = new InteractiveSessionClient(livyURL, livyConf, restartDeadSession, restClient, currentSessionId);
            return client;
        }
    }

    @Getter
    public static class SessionInfo {
        private int id;
        private String appId;
        private String appTag;
        private String webUIAddress;
        private String owner;
        private String proxyUser;
        private SessionState state;
        private SessionKind kind;
        private Map<String, String> appInfo;
        private List<String> log;

        public boolean isReady() {
            return SessionState.IDLE.equals(state);
        }

        public boolean isFinished() {
            return SessionState.ERROR.equals(state) ||
                    SessionState.DEAD.equals(state) ||
                    SessionState.SUCCESS.equals(state) ||
                    SessionState.KILLED.equals(state) ||
                    SessionState.SHUTTING_DOWN.equals(state);
        }

        public static SessionInfo fromJson(String json) throws IOException {
            SessionInfo sessionInfo = JsonUtil.decode(json, SessionInfo.class);
            return sessionInfo;
        }
    }

    @Data
    public static class SessionLog {
        private int id;
        private int from;
        private int size;
        private List<String> log;

        public static SessionLog fromJson(String json) throws IOException {
            return JsonUtil.decode(json, SessionLog.class);
        }

        public String logs() {
            return log.stream().collect(Collectors.joining("\n"));
        }
    }

    @Data
    @AllArgsConstructor
    public static class StatementRequest {
        private String code;
        private String kind;
    }

    @Data
    public static class StatementInfo {
        private Integer id;
        private StatementState state;
        private String tags;
        private double progress;
        private StatementOutput output;
        private Map<Integer, Double> jobProgress;

        public static StatementInfo fromJson(String json) throws IOException {
            return JsonUtil.decode(json, StatementInfo.class);
        }

        public boolean isAvailable() {
            return StatementState.AVAILABLE.equals(state) || StatementState.CANCELLED.equals(state);
        }

        @Data
        public static class StatementOutput {
            private String status;
            @JsonProperty(value = "execution_count")
            private String executionCount;
            private OutPutData data;
            private String ename;
            private String evalue;
            private String[] traceback;
            private TableMagic tableMagic;

            public boolean isError() {
                return this.status.equals("error");
            }

            @Data
            public static class TableMagic {
                private List<Map> headers;
                @JsonProperty(value = "data")
                private List<List> records;
            }

            @Data
            public static class OutPutData {
                @JsonProperty(value ="text/plain")
                private String plainText;
                @JsonProperty(value = "image/png")
                private String imagePng;
                @JsonProperty(value = "application/json")
                private QueryResults applicationJson;
                @JsonProperty(value = "application/vnd.livy.table.v1+json")
                private TableMagic applicationLivyTableJson;
            }

            @Data
            public static class QueryResults {
                private Schema schema;
                private List<List<Object>> data;
            }

            @Data
            public static class Schema {
                private String type;
                private List<Field> fields;
            }

            @Data
            public static class Field {
                private String name;
                private String type;
                private boolean nullable;
                private Object metadata;
            }
        }
    }
}
