package it.sssupserver.app.handlers.simplecdnhandler;

import java.net.URL;

/**
 * Simple class used to track a possible peer and monitor keepalives.
 * we have to look for
 *
 * PeerWatcher cross the following states:
 *  - Initialization
 *  - Discovery
 *  - KeepAlive
 *  - Shutdown
 */
public class PeerWatcher implements Comparable<PeerWatcher> {

    // URL used to query for peer status
    private URL url;
    public URL getUrl() {
        return url;
    }

    private DataNodeDescriptor datanode;
    public DataNodeDescriptor getDatanode() {
        return datanode;
    }
    public void setDatanode(DataNodeDescriptor datanode) {
        this.datanode = datanode;
    }

    // is this object associated with remote recognised peer?
    public boolean found() {
        return datanode != null;
    }

    public PeerWatcher(URL url) {
        this.url = url;
    }

    @Override
    public int compareTo(PeerWatcher o) {
        if (o == null) {
            throw new NullPointerException("Bad argument");
        }
        return getUrl().toString().compareTo(o.getUrl().toString());
    }
}
