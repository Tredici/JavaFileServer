package it.sssupserver.app.base;

import java.util.Collection;

import java.util.List;

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
            var s = content.stream().map(p -> new Node(p));
            children = s.toArray(Node[]::new);
            return s.filter(n -> n.isDirectory()).toArray(Node[]::new);
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
}
