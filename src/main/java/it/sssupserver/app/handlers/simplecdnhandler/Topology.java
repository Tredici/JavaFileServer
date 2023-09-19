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
        // prevent reace condition
        if (node == null) {
            return null;
        }
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
        // prevent reace condition
        if (node == null) {
            return null;
        }
        // any successor?
        var succE = datanodes.ceilingEntry(node.id+1);
        if (succE == null) {
            // search for last
            succE = datanodes.firstEntry();
        }
        // return other or me
        return succE.getValue() == node ? null : succE.getValue();
    }
    
    public DataNodeDescriptor getFileOwner(long hash)
    {
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

    public List<DataNodeDescriptor> getFileSuppliers(long hash) {
        // expected number o
        var R = thisnode.replicationFactor;
        List<DataNodeDescriptor> ans = new ArrayList<>(R);
        final var owner = getFileOwner(hash);
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
    public DataNodeDescriptor peekRandomSupplier(long hash) {
        var candidates = getFileSuppliers(hash);
        var node = candidates.get((int)(candidates.size() * Math.random()));
        return node;
    }

    // is given node supplier of file?
    public boolean isFileSupplier(DataNodeDescriptor node, long hash) {
        return getFileSuppliers(hash).contains(node);
    }

    public DataNodeDescriptor searchDataNodeDescriptorById(long id) {
        return datanodes.get(id);
    }

    // is current node supplier of file?
    public boolean isFileSupplier(long hash) {
        return isFileSupplier(thisnode, hash);
    }

    // Does given datanode own specified file?
    public boolean isFileOwner(DataNodeDescriptor datanode, long hash) {
        return datanode == getFileOwner(hash);
    }

    // is the file owned by the current node?
    public boolean isFileOwned(long hash) {
        return thisnode == getFileOwner(hash);
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
