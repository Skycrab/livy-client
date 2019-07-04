package com.mt.fbi.livy.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

/**
 * Created by yihaibo on 2019-04-18.
 */
public enum SessionState {
    /**
     * Session has not been started
     */
    NOT_STARTED("not_started"),

    /**
     * Session is starting
     */
    STARTING("starting"),

    /**
     * Session is waiting for input
     */
    IDLE("idle"),

    /**
     * Session is executing a statement
     */
    BUSY("busy"),

    /**
     * Session is shutting down
     */
    SHUTTING_DOWN("shutting_down"),

    /**
     * Session errored out
     */
    ERROR("error"),

    /**
     * Session has exited
     */
    DEAD("dead"),

    /**
     * Session has been killed
     */
    KILLED("killed"),

    /**
     * Session is successfully stopped
     */
    SUCCESS("success"),
    ;

    @JsonValue
    private String state;

    SessionState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    @JsonCreator
    public static SessionState getSessionState(@NonNull String state) {
        for(SessionState sessionState : values()) {
            if(sessionState.getState().equals(state)) {
                return sessionState;
            }
        }
        throw new IllegalArgumentException("no this state:" + state);
    }
}
