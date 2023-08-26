package it.sssupserver.app.base;

import java.io.PrintStream;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;

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

        public Path getPath() {
            return path;
        }

        public Node(Path path) {
            this.path = path;
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
            children = content.stream().map(p -> new Node(p)).toArray(Node[]::new);
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
    }


    // root of current FileTree, might be different from "/"
    private Node root;

    public Node getRoot() {
        return root;
    }

    public FileTree(Path path) {
        root = new Node(path);
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
}
