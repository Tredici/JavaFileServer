package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;
import java.net.URL;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration.HttpEndpoint;

public class HttpEndpointGson
    implements JsonDeserializer<HttpEndpoint>,
    JsonSerializer<HttpEndpoint> {
    @Override
    public HttpEndpoint deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var urlField = json.getAsJsonObject().get("Url");
        var urlObj = (URL)context.deserialize(urlField, URL.class);
        return new HttpEndpoint(urlObj);
    }

    @Override
    public JsonElement serialize(HttpEndpoint src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.add("Url", context.serialize(src.getUrl()));
        return jObj;
    }
}
