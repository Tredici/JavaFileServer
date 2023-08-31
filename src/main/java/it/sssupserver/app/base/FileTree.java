package it.sssupserver.app.base;

import java.io.PrintStream;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

// Data structure used to represent a file system
// It is used to obtain a snapshot of current state
// of files and directories mantained by a FileManager
public class FileTree {

    public class Node {
        // path referring to directory
        private Path path;
        // array of child nodes, available only for
        // directories
        private Node[] children;
        // reference to the parent direcory
        private Node parentNode;

        // helper properties used to associate file hash
        // (must be calculated using other tools) to
        // this node
        private String hashAlgorithm;
        private byte[] fileHash;

        // UTC timestamp associated with
        public Instant lastModified;

        public Instant getLastModified() {
            return lastModified;
        }

        public void setLastModified(Instant lastModified) {
            this.lastModified = lastModified;
        }

        // -1 means unknown size
        private long size = -1;

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public void setFileHash(String algorithm, byte[] fileHash) {
            this.hashAlgorithm = algorithm;
            this.fileHash = fileHash;
        }

        public byte[] getFileHash() {
            return fileHash;
        }

        public String getHashAlgorithm() {
            return hashAlgorithm;
        }

        public Node getParentNode() {
            return parentNode;
        }

        public Path getPath() {
            return path;
        }

        /**
         * Check new name is valid and change if only if
         * it change only the basename.
         * Cannot rename diretories.
         * Is a rename (i.e.: change only basename), NOT a move!
         */
        public void rename(Path newPath) {
            // check tipe equivalence
            if (newPath.isDir() != getPath().isDir()) {
                throw new IllegalArgumentException("Current and new path must refer to the same type of object");
            }
            // only for regular files
            if (!isRegularFile()) {
                throw new RuntimeException("Cannot rename non-regular file: " + this.toString());
            }
            // same path
            if (this.path.getPath().length != newPath.getPath().length) {
                throw new IllegalArgumentException("Path of different lengh: '"
                    + this.path.getPath() + "' and '"
                    + newPath.getPath().toString() + "'");
            }
            for (var i=0; i<this.getPath().getPath().length-1; ++i) {
                if (!this.path.getPath()[i].equals(newPath.getPath()[i])) {
                    throw new IllegalArgumentException("Mismatch between: '"
                    + this.path.getPath() + "' and '"
                    + newPath.getPath().toString() + "'");
                }
            }
            // change path
            this.path = newPath.clone();
        }

        // special role assigned to root
        public boolean isRoot() {
            return this == FileTree.this.root;
        }

        public Node(Node parentNode, Path path) {
            this.parentNode = parentNode;
            this.path = path;
        }

        public boolean isRegularFile() {
            return !isDirectory();
        }

        public boolean isDirectory() {
            return path.isDir();
        }

        public Node[] getChildren() {
            return children;
        }

        // how many children for the current node
        public int countChildren() {
            return children == null ? 0 : children.length;
        }

        // return live objects, i.e. the caller can
        // modify the tree by accessing returned objects
        // WARNING: replace previously set tree
        public Node[] setChildrenAndReturnSubdirs(Collection<Path> content) {
            children = content.stream().map(p -> new Node(this, p)).toArray(Node[]::new);
            return Arrays.stream(children).filter(n -> n.isDirectory()).toArray(Node[]::new);
        }

        public void print(PrintStream out, int depth) {
            for (int i = 0; i<depth; ++i) {
                out.print("    ");
            }
            var path = getPath().getPath();
            if (depth == 0 && children != null) {
                out.println("> /");
            } else {
                out.println("> " + path[path.length-1] + (isDirectory() ? "/" : ""));
            }
            if (children != null) for (Node node : children) {
                node.print(out, depth+1);
            }
        }

        // Recursively call fun on all Node(s) of the tree
        public void dfs(Consumer<Node> fun) {
            fun.accept(this);
            if (children != null) {
                for (var c : children) {
                    c.dfs(fun);
                }
            }
        }

        // perform a dfs to get path to all nodes (files)
        // respecting given predicate (test == null means take all)
        private void dfs(List<Path> ans, Predicate<Node> test) {
            if (test == null || test.test(this)) {
                ans.add(this.getPath());
            }
            if (children != null) {
                for (var c : children) {
                    c.dfs(ans, test);
                }
            }
        }

        // to obtain references to Node(s)
        private void dfsNodes(List<Node> ans, Predicate<Node> test) {
            if (test == null || test.test(this)) {
                ans.add(this);
            }
            if (children != null) {
                for (var c : children) {
                    c.dfsNodes(ans, test);
                }
            }
        }
    }


    // root of current FileTree, might be different from "/"
    private Node root;

    public Node getRoot() {
        return root;
    }

    public FileTree(Path path) {
        root = new Node(null, path);
    }

    public void print(PrintStream out) {
        getRoot().print(out, 0);
        //out.println("> " + getRoot().getPath().toString());
    }

    public void print() {
        print(System.out);
    }

    static public class FileTreeSerializer implements JsonSerializer<FileTree> {
        @Override
        public JsonElement serialize(FileTree src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            if (src.getRoot() != null) {
                jObj.add("Root", context.serialize(src.getRoot()));
            }
            return jObj;
        }
    }

    static public class NodeSerializer implements JsonSerializer<Node> {
        @Override
        public JsonElement serialize(Node src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.addProperty("Path", src.getPath().toString());
            jObj.add("IsDirectory", new JsonPrimitive(src.isDirectory()));
            if (src.isDirectory()) {
                var jArray = new JsonArray(src.countChildren());
                if (src.countChildren() > 0) {
                    var children = src.getChildren();
                    for (int i=0; i < children.length; ++i) {
                        jArray.add(context.serialize(children[i]));
                    }
                }
                jObj.add("Children", jArray);
            }
            return jObj;
        }
    }

    public String toJson(boolean preattyPrint) {
        // Reference:
        //  https://github.com/google/gson/blob/main/UserGuide.md
        var gBuilder = new GsonBuilder()
                    .registerTypeAdapter(FileTree.class, new FileTreeSerializer())
                    .registerTypeAdapter(Node.class, new NodeSerializer());
        if (preattyPrint) {
            gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();

        return gson.toJson(this);
    }

    public String toJson() {
        return toJson(false);
    }

    public Node[] getAllNodes() {
        List<Node> ans = new ArrayList<>();
        if (getRoot() != null) {
            getRoot().dfsNodes(ans, n -> true);
        }
        return ans.toArray(new Node[0]);
    }

    // return only paths to regular files (not directory)
    public Path[] getRegularFiles() {
        List<Path> ans = new ArrayList<>();
        if (getRoot() != null) {
            getRoot().dfs(ans, n -> !n.isDirectory());
        }
        return ans.toArray(new Path[0]);
    }

    public Node[] getRegularFileNodes() {
        List<Node> ans = new ArrayList<>();
        if (getRoot() != null) {
            getRoot().dfsNodes(ans, n -> !n.isDirectory());
        }
        return ans.toArray(new Node[0]);
    }

    public Path[] getDirectories() {
        List<Path> ans = new ArrayList<>();
        if (getRoot() != null) {
            getRoot().dfs(ans, n -> n.isDirectory());
        }
        return ans.toArray(new Path[0]);
    }

    public Node[] getDirectorysNodes() {
        List<Node> ans = new ArrayList<>();
        if (getRoot() != null) {
            getRoot().dfsNodes(ans, n -> n.isDirectory());
        }
        return ans.toArray(new Node[0]);
    }

    // perform a dfs on all the nodes inside the tree and
    // apply fun to all them
    public void dfs(Consumer<Node> fun) {
        if (root == null) {
            return;
        }
        fun.accept(root);
    }

    public List<Node> filter(Predicate<Node> fun) {
        List<Node> ans = new ArrayList<>();
        if (root != null) {
            root.dfsNodes(ans, fun);
        }
        return ans;
    }
}
