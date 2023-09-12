package it.sssupserver.app.handlers.simplecdnhandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.gson.GsonBuilder;

// this class will hold all the informations about
// the ring topology
public class Topology {
    private ConcurrentSkipListMap<Long, DataNodeDescriptor> datanodes = new ConcurrentSkipListMap<>();

    /**
     * Search and return node by id
     */
    public DataNodeDescriptor findDataNodeDescriptorById(long id) {
        return datanodes.get(id);
    }

    /**
     * Search and remove node by id
     */
    public DataNodeDescriptor removeDataNodeDescriptorById(long id) {
        return datanodes.remove(id);
    }

    private SimpleCDNHandler handler;
    private DataNodeDescriptor thisnode;
    // default topology is only me
    public Topology(SimpleCDNHandler handler) {
        this.handler = handler;
        this.thisnode = handler.getThisnode();
        if (thisnode == null) {
            throw new RuntimeException("SimpleCDNHandler.this.thisnode must not be null!");
        }
        datanodes.put(thisnode.id, thisnode);
    }

    // null means thisnode
    public DataNodeDescriptor findPrevious(DataNodeDescriptor node) {
        // any previous?
        var prevE = datanodes.floorEntry(node.id-1);
        if (prevE == null) {
            // search for last
            prevE = datanodes.lastEntry();
        }
        // return other or me
        return prevE.getValue() == node ? null : prevE.getValue();
    }

    // null means thisnode
    public DataNodeDescriptor findSuccessor(DataNodeDescriptor node) {
        // any successor?
        var succE = datanodes.ceilingEntry(node.id+1);
        if (succE == null) {
            // search for last
            succE = datanodes.firstEntry();
        }
        // return other or me
        return succE.getValue() == node ? null : succE.getValue();
    }

    // get hash from file name
    public long getFileHash(String path) {
        return (long)path.hashCode();
    }

    public DataNodeDescriptor getFileOwner(String path) {
        // get file hash
        var hash = getFileHash(path);
        var ownerE = datanodes.floorEntry(hash);
        if (ownerE == null) {
            ownerE = datanodes.lastEntry();
        }
        var candidateOwner = ownerE.getValue();
        // owner must be running!
        if (candidateOwner.getStatus() != DataNodeDescriptor.Status.RUNNING) {
            // prevent allocation if not necessary
            Set<DataNodeDescriptor> loopPrevention = new HashSet<>();
            do {
                // loop prevention
                if (!loopPrevention.add(candidateOwner)) {
                    return null;
                }
                // try previous
                candidateOwner = findPrevious(candidateOwner);
            } while (candidateOwner.getStatus() != DataNodeDescriptor.Status.RUNNING);
        }
        return candidateOwner;
    }

    public List<DataNodeDescriptor> getFileSuppliers(String path) {
        // expected number o
        var R = thisnode.replication_factor;
        List<DataNodeDescriptor> ans = new ArrayList<>(R);
        final var owner = getFileOwner(path);
        ans.add(owner);
        var supplier = owner;
        Set<DataNodeDescriptor> loopPrevention = new HashSet<>(R);
        while (ans.size() < R) {
            supplier = findPrevious(supplier);
            if (supplier == null || loopPrevention.contains(supplier)) {
                // avoid looping - this strategy (i.e. check all instead of checking
                // if owner refound) is ok also in case of concurrent topology changes
                break;
            }
            loopPrevention.add(supplier);
            if (supplier.getStatus() == DataNodeDescriptor.Status.RUNNING) {
                // only RUNNING nodes can figure as suppliers
                ans.add(supplier);
            }
        }
        return ans;
    }

    // obtain random supplier to perform redirect
    public DataNodeDescriptor peekRandomSupplier(String path) {
        var candidates = getFileSuppliers(path);
        var node = candidates.get((int)(candidates.size() * Math.random()));
        return node;
    }

    // is given node supplier of file?
    public boolean isFileSupplier(DataNodeDescriptor node, String path) {
        return getFileSuppliers(path).contains(node);
    }

    public DataNodeDescriptor searchDataNodeDescriptorById(long id) {
        return datanodes.get(id);
    }

    // is current node supplier of file?
    public boolean isFileSupplier(String path) {
        return isFileSupplier(thisnode, path);
    }

    // Does given datanode own specified file?
    public boolean isFileOwner(DataNodeDescriptor datanode, String path) {
        return datanode == getFileOwner(path);
    }

    // is the file owned by the current node?
    public boolean isFileOwned(String path) {
        return thisnode == getFileOwner(path);
    }

    public DataNodeDescriptor[] getSnapshot() {
        return datanodes.values().toArray(new DataNodeDescriptor[0]);
    }

    public String asJsonArray(boolean prettyPrinting) {
        var snapshot = getSnapshot();
        var gBuilder = new GsonBuilder();
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .create();
        var jSnapshot = gson.toJson(snapshot);
        return jSnapshot;
    }

    public String asJsonArray() {
        return asJsonArray(false);
    }

    /**
     * Be R the replication factor used by the ring.
     * This function is used to get the (if R is big enoug)
     * R-1 successors and predecessors of the current node,
     * this is because neighbours (these special nodes)
     * need to be keep in sync more often than remote nodes.
     */
    public List<DataNodeDescriptor> getNeighboours(DataNodeDescriptor centralNode, long R) {
        List<DataNodeDescriptor> ans = new ArrayList<>();
        DataNodeDescriptor pred = centralNode, succ = centralNode;
        for (var i=1; i!=R; ++i) {
            pred = findPrevious(pred);
            if (pred == null || ans.contains(pred)) {
                // loop!
                break;
            }
            ans.add(pred);
            succ = findSuccessor(succ);
            if (succ == null || ans.contains(succ)) {
                // loop!
                break;
            }
            ans.add(succ);
        }
        return ans;
    }

    /**
     * Register a new node in the known topology
     */
    public DataNodeDescriptor addDataNodeDescriptor(DataNodeDescriptor descriptor) {
        var ans = datanodes.put(descriptor.getId(), descriptor);
        handler.updateLastTopologyUpdateTimestamp();
        return ans;
    }

    /**
     * Remove DataNodeDescriptor from map, but assert the item
     * to be removed has not been changed
     * Return true if the item has been removed (or if nothing
     * is remained), return false if the item was not anymore
     * inside the map
     */
    public boolean removeDataNodeDescriptor(DataNodeDescriptor descriptor) {
        var ans = datanodes.compute(descriptor.getId(), (k,v) -> {
            if (v == descriptor) {
                handler.updateLastTopologyUpdateTimestamp();
                return null;
            }
            return v;
        }) == null;
        return ans;
    }
}
