package it.sssupserver.app.base;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

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
        public Node[] setChilderAndReturnSubdirs(Collection<Path> content) {
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
}
