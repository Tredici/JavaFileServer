package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.net.URL;

import com.google.gson.*;

import it.sssupserver.app.handlers.simplecdnhandler.gson.HttpEndpointGson;
import it.sssupserver.app.handlers.simplecdnhandler.gson.SimpleCDNConfigurationGson;

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

        @Override
        public String toString() {
            return url.toString();
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

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public HttpEndpoint[] getClientEndpoints() {
        return clientEndpoints;
    }

    public void setClientEndpoints(HttpEndpoint[] clientEndpoints) {
        this.clientEndpoints = clientEndpoints;
    }

    public HttpEndpoint[] getManagementEndpoints() {
        return managementEndpoints;
    }

    public void setManagementEndpoints(HttpEndpoint[] managementEndpoints) {
        this.managementEndpoints = managementEndpoints;
    }

    public HttpEndpoint[] getCandidatePeers() {
        return candidatePeers;
    }

    public void setCandidatePeers(HttpEndpoint[] candidatePeers) {
        this.candidatePeers = candidatePeers;
    }

    public SimpleCDNConfiguration() {

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
        var gBuilder = new GsonBuilder()
            .registerTypeAdapter(SimpleCDNConfiguration.class, new SimpleCDNConfigurationGson())
            .registerTypeAdapter(HttpEndpoint.class, new HttpEndpointGson());
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
