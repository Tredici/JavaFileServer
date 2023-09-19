package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration.HttpEndpoint;


public class SimpleCDNConfigurationGson
    implements JsonDeserializer<SimpleCDNConfiguration>,
    JsonSerializer<SimpleCDNConfiguration> {

    private static HttpEndpoint[] parseHttpEndpoints(JsonArray jHttpEndpoints, JsonDeserializationContext context) {
        var httpEndpoints = new HttpEndpoint[jHttpEndpoints.size()];
        for (int i=0; i!=httpEndpoints.length; ++i) {
            httpEndpoints[i] = (HttpEndpoint)context.deserialize(jHttpEndpoints.get(i), HttpEndpoint.class);
        }
        return httpEndpoints;
    }

    @Override
    public SimpleCDNConfiguration deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var jobj = json.getAsJsonObject();
        var ans = new SimpleCDNConfiguration();
        ans.setNodeId(jobj.get("NodeId").getAsLong());
        ans.setReplicationFactor(jobj.get("ReplicationFactor").getAsInt());
        ans.setUser(jobj.get("User").getAsString());

        var jClientEndpoints = jobj.get("ClientEndpoints").getAsJsonArray();
        var clientEndpoints = parseHttpEndpoints(jClientEndpoints, context);
        var jManagementEndpoints = jobj.get("ManagementEndpoints").getAsJsonArray();
        var managementEndpoints = parseHttpEndpoints(jManagementEndpoints, context);
        var jCandidatePeers = jobj.get("CandidatePeers").getAsJsonArray();
        var candidatePeers = parseHttpEndpoints(jCandidatePeers, context);

        ans.setClientEndpoints(clientEndpoints);
        ans.setManagementEndpoints(managementEndpoints);
        ans.setCandidatePeers(candidatePeers);

        return ans;
    }

    @Override
    public JsonElement serialize(SimpleCDNConfiguration src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("NodeId", src.getnodeId());
        jObj.addProperty("ReplicationFactor", src.getReplicationFactor());
        jObj.addProperty("User", src.getUser());
        {
            var ces = src.getClientEndpoints();
            var jArray = new JsonArray(ces.length);
            for (var ce : ces) {
                jArray.add(context.serialize(ce));
            }
            jObj.add("ClientEndpoints", jArray);
        }
        {
            var mes = src.getManagementEndpoints();
            var jArray = new JsonArray(mes.length);
            for (var me : mes) {
                jArray.add(context.serialize(me));
            }
            jObj.add("ManagementEndpoints", jArray);
        }
        {
            var cps = src.getCandidatePeers();
            var jArray = new JsonArray(cps.length);
            for (var cp : cps) {
                jArray.add(context.serialize(cp));
            }
            jObj.add("CandidatePeers", jArray);
        }
        return jObj;
    }
}