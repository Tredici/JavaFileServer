package it.sssupserver.app.commands.utils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import it.sssupserver.app.base.FileTree;
import it.sssupserver.app.base.InvalidPathException;
import it.sssupserver.app.base.Path;
import it.sssupserver.app.base.FileTree.Node;
import it.sssupserver.app.commands.ListCommand;
import it.sssupserver.app.commands.schedulables.SchedulableListCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

/**
 * This class expose a sigle static method
 * used to iterate
 */
public class ListTreeCommand extends SchedulableListCommand {
    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }

    // only one of all the related instances will effectively act
    // on them
    private CompletableFuture<FileTree> fsSnaphot;
    private FileTree tree;

    // while traversing FileTree, this node refer to currently
    // explored directory (we are checking if given directory exists)
    //
    // If tree==null, then we are checking if root directory exists
    // (by default it should be available, otherwise everithing should fail)
    //
    // If parentNode is not null we are exploring the content of a directory
    // looking for files and subdirectories, in that case counter
    private FileTree.Node parentNode;

    // shared counter across all parallel instances, used to test when all
    // paths have been explored and assert task completion
    private AtomicInteger counter;

    // reference to FileManager, used for recursion
    private FileManager executor;

    private ListTreeCommand(FileManager executor, Path path) {
        super(new ListCommand(path));
        fsSnaphot = new CompletableFuture<>();
        this.executor = executor;
    }

    private ListTreeCommand(FileManager executor) throws InvalidPathException {
        this(executor, new Path(""));
    }

    // recursion constructor - all property shared except
    // for parentNode (associated to getPath(), then logically unique)
    private ListTreeCommand(
        FileManager executor,
        Identity user,
        CompletableFuture<FileTree> fsSnaphot,
        FileTree tree,
        AtomicInteger counter,
        // only difference - getPath() to init super()
        Node subdir
        ) {
        super(new ListCommand(subdir.getPath()));
        // new reference
        this.parentNode = subdir;
        // shared properties across al related instances
        this.executor = executor;
        this.user = user;
        this.fsSnaphot = fsSnaphot;
        this.tree = tree;
        this.counter = counter;
    }

    public static Future<FileTree> explore(FileManager executor, String path, Identity user) throws Exception {
        return explore(executor, new Path(path), user);
    }

    public static Future<FileTree> explore(FileManager executor, Path path, Identity user) throws Exception {
        var cmd = new ListTreeCommand(executor, path);
        if (user != null) {
            cmd.setUser(user);
        }
        executor.scheduleExecution(cmd);
        return cmd.fsSnaphot;
    }

    private void decrementCounter(int n) {
        // avoid memory access if unnecessary
        if (n != 0) {
            var cnt = counter.addAndGet(-n); // n less item to check
            if (cnt == 0) {
                // traversal completed, return TreeFile
                fsSnaphot.complete(tree);
            }
        }
    }

    private void decrementCounter() {
        decrementCounter(1); // one less item to check
    }
    
    @Override
    public void reply(Collection<Path> content) throws Exception {
        // if this is the first traversed node initialize other variables
        if (tree == null) {
            // construct tree and add root
            tree = new FileTree(getPath());
            parentNode = tree.getRoot();
            // start traversing file tree - init counter to 1 because
            // it will be soon decremented
            counter = new AtomicInteger(1);
        }
        // how many subdirs to iterate on?
        var subdirs = parentNode.setChilderAndReturnSubdirs(content);
        // decrement by 1 (this node)
        // schedule more subdirs.lenght inspections
        decrementCounter(1 - subdirs.length); // note - only 1 subdir imply nop
        // for all children directories, schedule a new iteration
        for (Node subdir : subdirs) {
            // recursive scheduling of commands
            var newCommand = new ListTreeCommand(executor, user, fsSnaphot, tree, counter, subdir);
            this.executor.scheduleExecution(newCommand);
        }
    }

    @Override
    public void notFound() throws Exception {
        // if tree is null, then file traverse failed
        if (tree == null) {
            fsSnaphot.completeExceptionally(new Throwable("Not found: " + getPath().toString()));
        } else {
            // just decrease counter
            decrementCounter();
        }
    }
}
