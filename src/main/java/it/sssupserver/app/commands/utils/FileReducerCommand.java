package it.sssupserver.app.commands.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

import it.sssupserver.app.base.InvalidPathException;
import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.ReadCommand;
import it.sssupserver.app.commands.schedulables.SchedulableReadCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

/**
 * Simple class used to accumulate on a file, e.g.
 * in order to calculate an hash function on it.
 *
 * Reference for hash functions:
 *  https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/security/MessageDigest.html
 */
public class FileReducerCommand<T, U, V> extends SchedulableReadCommand {

    // String constants used to specify hash to be calculated
    public static final String MD5     = "MD5";
    public static final String SHA_1   = "SHA-1";
    public static final String SHA_256 = "SHA-256";

    private static final String[] availableHashAlgorithms = new String[]{
        MD5,
        SHA_1,
        SHA_256
    };

    public static String[] getAvailableHashAlgorithms() {
        return Arrays.copyOf(availableHashAlgorithms, availableHashAlgorithms.length);
    }

    /**
     * These 3 members are used to perform reduction on file
     * content:
     *  - accumulator
     *          Old partial value - initial value at the beginning
     *          can start as null
     *  - reducer
     *  - finalizer
     */
    private T accumulator;
    private BiFunction<T, ByteBuffer, T> reducer;
    private Function<T, V> finalizer;

    // Future used to communicate result to caller
    protected CompletableFuture<V> future;

    protected FileReducerCommand(
        ReadCommand cmd,
        T accumulator,
        BiFunction<T, ByteBuffer, T> reducer,
        Function<T, V> finalizer
        ) {
        super(cmd);
        future = new CompletableFuture<>();
        this.accumulator = accumulator;
        this.reducer = reducer;
        this.finalizer = finalizer;
    }

    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }

    @Override
    public void partial(ByteBuffer[] data) throws Exception {
        for (var byteBuffer : data) {
            accumulator = reducer.apply(accumulator, byteBuffer);
        }
    }

    @Override
    public void reply(ByteBuffer[] data) throws Exception {
        partial(data);
        future.complete(finalizer.apply(accumulator));
    }

    @Override
    public void notFound() throws Exception {
        future.completeExceptionally(new RuntimeException("File not found: " + getPath().toString()));
    }

    public static <T, V> Future<V> reduce(FileManager executor, String path, Identity user, T accumulator, BiFunction<T, ByteBuffer, T> reducer, Function<T, V> finalizer) throws InvalidPathException, Exception {
        return reduce(executor, new Path(path, false), user, accumulator, reducer, finalizer);
    }

    public static <T, V> Future<V> reduce(FileManager executor, Path path, Identity user, T accumulator, BiFunction<T, ByteBuffer, T> reducer, Function<T, V> finalizer) throws Exception {
        var cmd = new FileReducerCommand<>(new ReadCommand(path), accumulator, reducer, finalizer);
        if (user != null) {
            cmd.setUser(user);
        }
        executor.scheduleExecution(cmd);
        return cmd.future;
    }

    public static Future<byte[]> reduceByHash(FileManager executor, Path path, Identity user, String algorithm) throws Exception {
        var accumulator = MessageDigest.getInstance(algorithm);
        var reducer = (BiFunction<MessageDigest, ByteBuffer, MessageDigest>) (MessageDigest a, ByteBuffer b) -> {
            a.update(b);
            return a;
        };
        var finalizer = (Function<MessageDigest,byte[]>) (MessageDigest a) -> a.digest();
        return reduce(executor, path, user, accumulator, reducer, finalizer);
    }
}
