package org.coreocto.dev.hf.serverapp.factory;

import com.google.gson.JsonObject;

public class ResponseFactory {

    public enum ResponseType {
        GENERIC_JSON_OK,
        GENERIC_JSON_ERR
    }

    public static JsonObject getResponse(ResponseFactory.ResponseType type) {
        if (type == ResponseType.GENERIC_JSON_OK) {
            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty("status", "ok");
            return jsonObj;
        } else if (type == ResponseType.GENERIC_JSON_ERR) {
            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty("status", "error");
        }

        return null;
    }
}
