package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class TopologyWithIdentityGson implements JsonSerializer<TopologyWithIdentityGson> {
    private DataNodeDescriptor identity;
    private DataNodeDescriptor[] snapshot;

    public TopologyWithIdentityGson(DataNodeDescriptor identity, DataNodeDescriptor[] snapshot) {
        this.identity = identity;
        this.snapshot = snapshot;
    }

    @Override
    public JsonElement serialize(TopologyWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.add("Identity", context.serialize(identity));
        jObj.add("Topology", context.serialize(snapshot));
        return jObj;
    }
}
