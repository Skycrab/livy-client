package com.mt.fbi.livy.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

/**
 * Created by yihaibo on 2019-04-18.
 */
public enum StatementState {
    /**
     * Statement is enqueued but execution hasn't started
     */
    WAITING("waiting"),

    /**
     * Statement is currently running
     */
    RUNNING("running"),

    /**
     * Statement has a response ready
     */
    AVAILABLE("available"),

    /**
     * Statement failed
     */
    ERROR("error"),

    /**
     * Statement is being cancelling
     */
    CANCELLING("cancelling"),

    /**
     * Statement is cancelled
     */
    CANCELLED("cancelled"),
    ;

    @JsonValue
    private String state;

    StatementState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    @JsonCreator
    public static StatementState getStatementState(@NonNull String state) {
        for(StatementState statementState : values()) {
            if(statementState.getState().equals(state)) {
                return statementState;
            }
        }
        throw new IllegalArgumentException("no this state:" + state);
    }
}
