package it.sssupserver.app.handlers.simplecdnhandler;

import java.net.URL;
import java.time.Instant;

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

    @Override
    public boolean equals(Object o) {
        if (this.getClass() == o.getClass()) {
            return this.getId() == ((DataNodeDescriptor)o).getId();
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
}