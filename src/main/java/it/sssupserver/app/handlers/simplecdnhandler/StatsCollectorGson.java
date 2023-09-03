package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


public class StatsCollectorGson implements JsonSerializer<SimpleCDNHandler.StatsCollector> {
    @Override
    public JsonElement serialize(SimpleCDNHandler.StatsCollector src, Type typeOfSrc, JsonSerializationContext context) {
        var stats = src.getFileStats();
        return context.serialize(stats);
    }
}