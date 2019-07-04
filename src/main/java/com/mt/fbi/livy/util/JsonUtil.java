package com.mt.fbi.livy.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by yihaibo on 2019-04-18.
 */
public final class JsonUtil {

    private static final ObjectMapper mapper = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES , false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS,false);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        mapper.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return mapper;
    }

    public static JsonNode toJsonNode(String jsonStr) throws IOException{
        return mapper.readTree(jsonStr);
    }

    public static <T> T decode(String jsonStr, Class<T> valueType) throws IOException{
        return mapper.readValue(jsonStr, valueType);
    }

    public static <T> T decode(String jsonStr, TypeReference valueTypeRef) throws IOException{
        return mapper.readValue(jsonStr, valueTypeRef);
    }

    public static String encode(Object o) throws IOException {
        return mapper.writeValueAsString(o);
    }

    private JsonUtil() {

    }
}
