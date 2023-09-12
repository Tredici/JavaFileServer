package it.sssupserver.app.handlers.simplecdnhandler;

import java.util.List;

import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.commands.utils.FutureDeleteCommand;
import it.sssupserver.app.commands.utils.FutureMkdirCommand;
import it.sssupserver.app.commands.utils.FutureMoveCommand;
import it.sssupserver.app.commands.utils.QueableCommand;
import it.sssupserver.app.commands.utils.QueableCreateCommand;

import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiConsumer;

/**
 * Auxiliary class used to schedule file downloads
 */
    /**
     * Auxiliary class used to schedule file downloads
     */
public class FileDownloader implements Runnable, BiConsumer<HttpResponse<Void>,Throwable>,
    Flow.Subscriber<List<ByteBuffer>>, HttpResponse.BodyHandler<Void> {

    private int errorCounter = 0;
    private Instant lastError;

    private it.sssupserver.app.base.Path downloadPath;
    private it.sssupserver.app.base.Path finalPath;
    private it.sssupserver.app.base.Path dirname;

    /**
     * Rename file after download
     */
    private void renameFileAfterDownload() throws InterruptedException, ExecutionException, Exception {
        FutureMoveCommand.move(handler.getFileManager(), downloadPath, finalPath, handler.getIdentity()).get();
    }

    /**
     * In case of error during download, remove bad file
     */
    private void deleteBadDownload() throws InterruptedException, ExecutionException, Exception {
        FutureDeleteCommand.delete(handler.getFileManager(), downloadPath, handler.getIdentity()).get();
    }

    private SimpleCDNHandler handler;
    private RemoteFileInfo fileToDownload;
    private Runnable onTermination;
    private List<DataNodeDescriptor> suppliers;
    public FileDownloader(SimpleCDNHandler handler, RemoteFileInfo fileToDownload, Runnable onTermination) {
        this.handler = handler;
        this.fileToDownload = fileToDownload;
        this.onTermination = onTermination;
        this.suppliers = fileToDownload.getCandidateSuppliers();
        // path used for download
        downloadPath = fileToDownload.getDownloadPath();
        finalPath = fileToDownload.getEffectivePath();
        dirname = fileToDownload.getSearchPath().getDirname();
    }

    private void onError() {
        ++errorCounter;
        lastError = Instant.now();
        // New download attempt
        if (!suppliers.isEmpty()) {
            // reschedule
            handler.getThreadPool().submit(this);
        } else {
            // release lock
            onTermination.run();
            failed = false;
        }
    }

    /**
     * Used from Flow.Subscriber
     */
    private boolean failed = false;
    @Override
    public void accept(HttpResponse<Void> res, Throwable err) {
        if (failed || err != null || (res != null && res.statusCode() != 200)) {
            onError();
        } else {
            // register new node
            var fsStatus = handler.getFsStatus();
            var newNode = fsStatus.addRegularFileNode(finalPath);
            newNode.setSize(fileToDownload.getBestVersion().getSize());
            newNode.setFileHash(fileToDownload.getBestVersion().getHashAlgorithm(), fileToDownload.getBestVersion().getFileHash());
            try {
                var lfi = fsStatus.addLocalFileInfo(newNode);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(1);
            }
            // ok, release lock
            onTermination.run();
        }
    }

    /**
     * Send http request
     */
    @Override
    public void run() {
        try {
            // peer candidate supplier
            var supplier = suppliers.remove(0);
            // remote endpoint
            var rem = supplier.getRandomManagementEndpointURL().toURI();
            // build download request
            var req = handler.buildFileDownloadGET(rem, fileToDownload);
            handler.getHttpClient()
                .sendAsync(req, this)
                .whenCompleteAsync(this, handler.getThreadPool());
        } catch (URISyntaxException e) {
            e.printStackTrace();
            // maybe reschedule
            onError();
        }
    }

    // How to handle bad response
    //  https://stackoverflow.com/questions/56025114/how-do-i-get-the-status-code-for-a-response-i-subscribe-to-using-the-jdks-httpc
    // Sample on doc:
    //  HttpRequest request = HttpRequest.newBuilder()
    //    .uri(URI.create("http://www.foo.com/"))
    //    .build();
    //  BodyHandler<Path> bodyHandler = (rspInfo) -> rspInfo.statusCode() == 200
    //    ? BodySubscribers.ofFile(Paths.get("/tmp/f"))
    //    : c.replacing(Paths.get("/NULL"));
    //  client.sendAsync(request, bodyHandler)
    //    .thenApply(HttpResponse::body)
    //    .thenAccept(System.out::println);
    // check result
    @Override
    public BodySubscriber<Void> apply(ResponseInfo responseInfo) {
        if (responseInfo.statusCode() == 200) {
            return BodyHandlers.fromSubscriber(this).apply(responseInfo);
        } else {
            return BodyHandlers.discarding().apply(responseInfo);
        }
    }

    /**
     * Handle response
     */
    @Override
    public void onComplete() {
        // assert checksum is ok
        if (!checkReceivedData()) {
            failed = true;
        } else
        try {
            renameFileAfterDownload();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Error occurred while parsing response
     */
    @Override
    public void onError(Throwable err) {
        try {
            deleteBadDownload();
            onError();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
    }

    QueableCommand command;

    @Override
    public void onNext(List<ByteBuffer> buffers) {
        for (var buf : buffers) {
            updateDownloadStats(buf);
            if (command == null) {
                // assert directory creation
                try {
                    FutureMkdirCommand.create(handler.getFileManager(), dirname, handler.getIdentity()).get();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                try {
                    command = QueableCreateCommand.submit(handler.getFileManager(),
                        downloadPath,
                        handler.getIdentity(),
                        BufferManager.getFakeWrapper(buf)
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            } else {
                command = command.enqueue(BufferManager.getFakeWrapper(buf));
            }
        }
    }

    private long receiveSize = 0;
    private MessageDigest hasher;
    private void updateDownloadStats(ByteBuffer buffer) {
        // get size of available data
        receiveSize += buffer.remaining();
        // update hash
        hasher.update(buffer.asReadOnlyBuffer());
    }

    @Override
    public void onSubscribe(Subscription sub) {
        receiveSize = 0;
        // download started, init checksum fields
        try {
            hasher = MessageDigest.getInstance(fileToDownload.getBestVersion().getHashAlgorithm());
            sub.request(Long.MAX_VALUE);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Checksum and file check
     */
    private boolean checkReceivedData() {
        try {
            if (!command.getFuture().get()) {
                return false;
            } else {
                // are size and hash ok?
                var v = fileToDownload.getBestVersion();
                return v.getSize() == receiveSize
                    && Arrays.equals(v.getFileHash(), hasher.digest());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
