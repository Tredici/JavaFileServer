package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;
import java.net.URL;
import java.time.Instant;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class DataNodeDescriptorGson
    implements JsonSerializer<DataNodeDescriptor>,
    JsonDeserializer<DataNodeDescriptor> {
    @Override
    public JsonElement serialize(DataNodeDescriptor src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("Id", src.getId());
        jObj.addProperty("ReplicationFactor", src.getReplicationFactor());
        jObj.add("DataEndpoints", context.serialize(src.dataendpoints));
        jObj.add("ManagerEndpoint", context.serialize(src.managerendpoint));
        jObj.addProperty("StartInstant", src.getStartInstant().toEpochMilli());
        jObj.addProperty("Status", src.status.toString());
        jObj.addProperty("LastFileUpdate", src.getLastFileUpdate().toEpochMilli());
        jObj.addProperty("LastStatusChange", src.getLastStatusChange().toEpochMilli());
        jObj.addProperty("LastTopologyUpdate", src.getLastTopologyUpdate().toEpochMilli());
        return jObj;
    }

    @Override
    public DataNodeDescriptor deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var jObj = json.getAsJsonObject();
        var ans = new DataNodeDescriptor();
        // read default properties
        ans.setId(jObj.get("Id").getAsLong());
        ans.setReplicationFactor(jObj.get("ReplicationFactor").getAsInt());
        ans.setStatus(DataNodeDescriptor.Status.valueOf(jObj.get("Status").getAsString()));
        ans.setStartInstant(Instant.ofEpochMilli(jObj.get("StartInstant").getAsLong()));
        ans.setLastFileUpdate(Instant.ofEpochMilli(jObj.get("LastFileUpdate").getAsLong()));
        ans.setLastTopologyUpdate(Instant.ofEpochMilli(jObj.get("LastTopologyUpdate").getAsLong()));
        ans.setLastStatusChange(Instant.ofEpochMilli(jObj.get("LastStatusChange").getAsLong()));
        {
            // read management endpoints
            var jArray = jObj.get("ManagerEndpoint").getAsJsonArray();
            var mes = (URL[])context.deserialize(jArray, URL[].class);
            ans.setManagerendpoint(mes);
        }
        {
            // read management endpoints
            var jArray = jObj.get("DataEndpoints").getAsJsonArray();
            var des = (URL[])context.deserialize(jArray, URL[].class);
            ans.setDataendpoints(des);
        }
        return ans;
    }
}
