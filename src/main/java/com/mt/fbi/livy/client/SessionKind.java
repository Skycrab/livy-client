package com.mt.fbi.livy.client;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

/**
 * Created by yihaibo on 2019-04-18.
 */
public enum SessionKind {

    /**
     * Interactive Scala Spark session
     */
    SPARK("spark"),

    /**
     * Interactive Python Spark session
     */
    PYSPARK("pyspark"),

    /**
     * Interactive R Spark session
     */
    SPARKR("sparkr"),

    /**
     * Interactive SQL Spark session
     */
    SQL("sql"),
    ;

    @JsonValue
    private String kind;

    SessionKind(String kind) {
        this.kind = kind;
    }

    public String getKind() {
        return kind;
    }

    @JsonCreator
    public static SessionKind getSessionState(@NonNull String kind) {
        for(SessionKind sessionKind : values()) {
            if(sessionKind.getKind().equals(kind)) {
                return sessionKind;
            }
        }
        throw new IllegalArgumentException("no this kind:" + kind);
    }
}
