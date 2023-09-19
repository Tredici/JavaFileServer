package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.handlers.simplecdnhandler.StatsCollector;
import it.sssupserver.app.handlers.simplecdnhandler.StatsCollector.FileStats;


public class StatsCollectorGson implements JsonSerializer<StatsCollector> {
    @Override
    public JsonElement serialize(StatsCollector src, Type typeOfSrc, JsonSerializationContext context) {
        var stats = src.getFileStats();
        return context.serialize(stats);
    }
}