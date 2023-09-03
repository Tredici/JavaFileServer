package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler.StatsCollector;

public class StatisticsWithIdentityGson implements JsonSerializer<StatisticsWithIdentityGson> {
    private DataNodeDescriptor identity;
    private StatsCollector.FileStats[] statistics;

    public StatisticsWithIdentityGson(DataNodeDescriptor identity, StatsCollector.FileStats[] statistics) {
        this.identity = identity;
        this.statistics = statistics;
    }

    @Override
    public JsonElement serialize(StatisticsWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.add("Identity", context.serialize(identity));
        jObj.add("Statistics", context.serialize(statistics));
        return jObj;
    }
}
