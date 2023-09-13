package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class FileStatsGson implements JsonSerializer<StatsCollector.FileStats> {
    @Override
    public JsonElement serialize(StatsCollector.FileStats src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("Path", src.getPath());
        jObj.addProperty("Supplied", src.getSupplied());
        jObj.addProperty("Redirects", src.getRedirects());
        jObj.addProperty("Errors", src.getErrors());
        return jObj;
    }
}
