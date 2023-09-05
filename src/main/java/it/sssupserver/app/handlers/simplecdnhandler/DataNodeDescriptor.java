package it.sssupserver.app.handlers.simplecdnhandler;

import java.net.URL;
import java.time.Instant;

import com.google.gson.GsonBuilder;

// this class
public class DataNodeDescriptor {
    // a DataNote can traverse all these status
    public enum Status {
        // node not working
        SHUTDOWN,
        // normal running status, traversed in this order
        STARTING,
        SYNCING,
        RUNNING,
        STOPPING,
        // error status
        MAYBE_FAILED,
        FAILED
    }
    // id identifing the node
    public long id;
    // list of: http://myendpoint:port
    // used to find http endpoints to download data (as client)
    public URL[] dataendpoints;
    // used to find http endpoint to operate consistency protocol
    public URL[] managerendpoint;
    // how many replicas for each file? Default: 3
    public int replication_factor = 3;
    // status of the node
    public Status status = Status.SHUTDOWN;

    /**
     * A node can go up and down multiple times,
     * this filed remember when it get up
     * last time
     */
    private Instant startInstant;

    /**
     * Timestamp updated by a node and sent in JSON responses
     * to evidence current node status changes. Important in
     * synchronization protocols.
     */
    private Instant lastStatusChange;

    public Instant getLastStatusChange() {
        return lastStatusChange;
    }
    public void setLastStatusChange(Instant lastStatusChangeInstant) {
        this.lastStatusChange = lastStatusChangeInstant;
    }

    /**
     * Last time this node was considered in an interaction
     * with the local node. Used in keepalive protocol.
     * Should not be serialized, it is used only locally.
     */
    private Instant keepAlive;
    /**
     * Last time topology seen by this node was updated.
     * Used in topology reconstuction protocols.
     * Updated only but the node itself. Sent via Json responses.
     */
    private Instant lastTopologyUpdate;
    /**
     * Last time files held by this node were modified/updated.
     * Used in file resynchronition protocol. Sent via Json responses.
     */
    private Instant lastFileUpdate;

    public Instant getLastFileUpdate() {
        return lastFileUpdate;
    }
    public void setLastFileUpdate(Instant lastFileUpdate) {
        this.lastFileUpdate = lastFileUpdate;
    }
    public Instant getLastTopologyUpdate() {
        return lastTopologyUpdate;
    }
    public void setLastTopologyUpdate(Instant lastTopologyUpdate) {
        this.lastTopologyUpdate = lastTopologyUpdate;
    }
    public Instant getKeepAlive() {
        return keepAlive;
    }
    public void setKeepAlive(Instant keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this.getClass() == o.getClass()) {
            var on = (DataNodeDescriptor)o;
            return this.getId() == on.getId()
                && this.getStartInstant().equals(on.getStartInstant());
        } else {
            return false;
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getReplicationFactor() {
        return replication_factor;
    }

    public void setReplicationFactor(int replication_factor) {
        this.replication_factor = replication_factor;
    }

    public URL[] getDataendpoints() {
        return dataendpoints;
    }

    public void setDataendpoints(URL[] dataendpoints) {
        this.dataendpoints = dataendpoints;
    }

    public URL[] getManagerendpoint() {
        return managerendpoint;
    }

    public void setManagerendpoint(URL[] managerendpoint) {
        this.managerendpoint = managerendpoint;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Instant getStartInstant() {
        return startInstant;
    }

    public void setStartInstant(Instant startInstant) {
        this.startInstant = startInstant;
    }

    public URL getRandomDataEndpointURL() {
        var l = dataendpoints.length;
        var de = dataendpoints[(int)(l*Math.random())];
        return de;
    }

    public URL getRandomManagementEndpointURL() {
        var l = managerendpoint.length;
        var de = managerendpoint[(int)(l*Math.random())];
        return de;
    }

    public static DataNodeDescriptor fromJson(String json) {
        DataNodeDescriptor ans;
        var gson = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptor())
            .create();
        ans = gson.fromJson(json, DataNodeDescriptor.class);
        return ans;
    }
}