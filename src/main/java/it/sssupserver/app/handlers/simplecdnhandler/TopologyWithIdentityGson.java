package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class TopologyWithIdentityGson
    implements JsonSerializer<TopologyWithIdentityGson>,
    JsonDeserializer<TopologyWithIdentityGson> {

    private DataNodeDescriptor identity;
    private DataNodeDescriptor[] snapshot;

    public DataNodeDescriptor getIdentity() {
        return identity;
    }

    public DataNodeDescriptor[] getSnapshot() {
        return snapshot;
    }

    private TopologyWithIdentityGson() {}

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

    @Override
    public TopologyWithIdentityGson deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var ans = new TopologyWithIdentityGson();
        var jObj = json.getAsJsonObject();
        if (jObj.has("Identity")) {
            ans.identity = context.deserialize(jObj.getAsJsonObject("Identity"), DataNodeDescriptor.class);
        }
        ans.snapshot = context.deserialize(jObj.getAsJsonArray("Topology"), DataNodeDescriptor[].class);
        return ans;
    }

    public static TopologyWithIdentityGson parse(String json) {
        var gson = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(TopologyWithIdentityGson.class, new TopologyWithIdentityGson())
            .create();
        var ans = gson.fromJson(json, TopologyWithIdentityGson.class);
        return ans;
    }
}
