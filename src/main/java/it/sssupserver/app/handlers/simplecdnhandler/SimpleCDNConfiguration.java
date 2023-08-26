package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.net.URL;

import com.google.gson.*;

// class used to parse CDN Datanode configuration
public class SimpleCDNConfiguration {
    
    public static class HttpEndpoint {
        private URL url;

        public HttpEndpoint(URL url) {
            this.url = url;
        }

        public URL getUrl() {
            return url;
        }
    }

    public static class HttpEndpointGson implements JsonDeserializer<HttpEndpoint> {
        @Override
        public HttpEndpoint deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            var urlField = json.getAsJsonObject().get("Url");
            var urlObj = (URL)context.deserialize(urlField, URL.class);
            return new HttpEndpoint(urlObj);
        }
    }

    public static class SimpleCDNConfigurationGson implements JsonDeserializer<SimpleCDNConfiguration> {

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

    }

    // id of current node
    private long nodeId;
    // how many replicas required
    private int replicationFactor;
    // used to distinguish CDN files from others
    private String user;

    // this node will listen for client connections on these endpoint
    private HttpEndpoint[] clientEndpoints;
    // this node will listen for other DataNode(s) connections on these endpoint
    private HttpEndpoint[] managementEndpoints;
    // this node will look for other DataNode(s) on these endpoint
    private HttpEndpoint[] candidatePeers;
    
    // liste di ip:porte di ascolto per client
    // liste di ip:porte di ascolto per altri nodi

    public long getnodeId() {
        return nodeId;
    }

    private void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    private void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getUser() {
        return user;
    }

    private void setUser(String user) {
        this.user = user;
    }

    public HttpEndpoint[] getClientEndpoints() {
        return clientEndpoints;
    }

    private void setClientEndpoints(HttpEndpoint[] clientEndpoints) {
        this.clientEndpoints = clientEndpoints;
    }

    public HttpEndpoint[] getManagementEndpoints() {
        return managementEndpoints;
    }

    private void setManagementEndpoints(HttpEndpoint[] managementEndpoints) {
        this.managementEndpoints = managementEndpoints;
    }

    public HttpEndpoint[] getCandidatePeers() {
        return candidatePeers;
    }

    private void setCandidatePeers(HttpEndpoint[] candidatePeers) {
        this.candidatePeers = candidatePeers;
    }

    private SimpleCDNConfiguration() {

    }

    public void assertConfiguration() {
        if (replicationFactor <= 0 || user == null) {
            throw new RuntimeException("Invalid configuration found");
        }
    }

    public static SimpleCDNConfiguration parseJsonCondifuration(String path) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
        SimpleCDNConfiguration ans = new SimpleCDNConfiguration();
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(HttpEndpoint.class, new HttpEndpointGson())
            .registerTypeAdapter(SimpleCDNConfiguration.class, new SimpleCDNConfigurationGson())
            .create();

        try (var fr = new FileReader(path)) {
            ans = gson.fromJson(fr, SimpleCDNConfiguration.class);
        } catch (Exception e) {
            System.err.println("Error parsing configuration file: " + path);
            System.err.println(e);
            System.exit(1);
        }

        return ans;
    }

    public String toJson(boolean prettyPrinting) {
        var gBuilder = new GsonBuilder();
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();
        var jconf = gson.toJson(this);
        return jconf;
    }

    public String toJson() {
        return toJson(false);
    }
}
