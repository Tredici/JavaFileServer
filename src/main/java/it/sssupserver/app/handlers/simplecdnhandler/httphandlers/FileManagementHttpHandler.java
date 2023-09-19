package it.sssupserver.app.handlers.simplecdnhandler.httphandlers;

import java.io.IOException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.base.BufferManager.BufferWrapper;
import it.sssupserver.app.commands.utils.FutureDeleteCommand;
import it.sssupserver.app.commands.utils.FutureMkdirCommand;
import it.sssupserver.app.commands.utils.FutureMoveCommand;
import it.sssupserver.app.commands.utils.QueableCommand;
import it.sssupserver.app.commands.utils.QueableCreateCommand;
import it.sssupserver.app.handlers.httphandler.HttpSchedulableReadCommand;
import it.sssupserver.app.handlers.simplecdnhandler.FilenameMetadata;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler.ManagedFileSystemStatus;
import java.net.URI;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Collections;
import static it.sssupserver.app.handlers.httphandler.HttpResponseHelpers.*;
import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;


// listen for PUT and DELETE commands
public class FileManagementHttpHandler implements HttpHandler {
    public static final String PATH = "/file";

    private SimpleCDNHandler handler;
    public FileManagementHttpHandler(SimpleCDNHandler handler) {
        this.handler = handler;
    }

    private void handleGET(HttpExchange exchange) throws Exception {
        // path to requsted file
        var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
        // missing file name corresponds to index.html
        if (requestedFile.isEmpty() || requestedFile.endsWith("/")) {
            requestedFile += "index.html";
        }
        // check if specific version of the file was required
        // extract metadata
        it.sssupserver.app.base.Path receivedPath = null;
        FilenameMetadata metadata = null;
        try {
            // try to parse received path
            receivedPath = new it.sssupserver.app.base.Path(requestedFile);
        } catch (Exception e) {
            httpBadRequest(exchange, "Bad path: " + requestedFile);
            return;
        }
        // does path contains metadata?
        metadata = new FilenameMetadata(receivedPath.getBasename());
        // get dirname
        var dirname = receivedPath.getDirname();
        // extract searchPath
        var searchPath = dirname.createSubfile(metadata.getSimpleName());
        if (!handler.testSupplyabilityOrRedirectToManagement(searchPath.toString(), exchange)) {
            // file not available here
            return;
        }
        // search file version by metadata
        var candidateVersion = handler.getFsStatus().searchVersionByMetadata(searchPath.toString(), metadata);
        if (candidateVersion == null) {
            // 404 NOT FOUND
            httpNotFound(exchange, "File not found: " + requestedFile);
            return;
        } else if (candidateVersion.isDeleted()) {
            httpGone(exchange);
            return;
        }
        // check ETah HTTP header
        // ETag parameter for the local file version
        var localETag = candidateVersion.generateHttpETagHeader();
        if (exchange.getRequestHeaders().containsKey("ETag"))
        {
            // ETag parameter found in request
            var reqETag = exchange.getRequestHeaders().getFirst("ETag");
            // if they are equals do not send response back
            if (localETag.equals(reqETag)) {
                // content not modified
                httpNotModified(exchange);
                return;
            }
        }
        // prepare to send back response
        var resH = exchange.getResponseHeaders();
        resH.set("ETag", localETag);
        // path of the file effectively returned
        var truePath = candidateVersion.getPath();
        // Try to detect MIME type
        var mime = handler.estimateMimeType(metadata.getSimpleName());
        var headers = Collections.singletonMap("Content-Type", mime);
        // send file back
        HttpSchedulableReadCommand.handle(handler.getFileManager(), exchange, handler.getIdentity(), SimpleCDNHandler.sanitazePath(truePath), headers);
        // DONE
    }

    private void handlePUT(HttpExchange exchange) throws Exception {
        // handle upload of file - a file is received by an admin application
        long receivedFileSize = 0;
        var md = MessageDigest.getInstance(ManagedFileSystemStatus.HASH_ALGORITHM);
        // extract path to file
        var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath().toString();
        it.sssupserver.app.base.Path receivedPath = null;
        FilenameMetadata metadata = null;
        // check if path is ok - no '@' inside, isValidPathName
        {
            boolean badPath = false;
            try {
                receivedPath = new it.sssupserver.app.base.Path(requestedFile);
                // extract medatada
                metadata = new FilenameMetadata(receivedPath.getBasename());
                if (metadata.holdMetadata() || requestedFile.contains("@")) {
                    badPath = true;
                }
                // assert it match file path
                if (!isValidPathName(receivedPath)) {
                    badPath = true;
                }
            } catch (Exception e) {
                badPath = true;
            }
            if (badPath) {
                httpBadRequest(exchange, "Bad path: " + requestedFile);
                return;
            }
        }
        var dirname = receivedPath.getDirname();
        // is this node owner of the file? Otherwise redirect
        if (!handler.testOwnershipOrRedirectToManagement(requestedFile, exchange)) {
            return;
        }
        // get timestamp used to track file
        metadata.setTimestamp(Instant.now());
        // generate temporary download file name - "@tmp"
        metadata.setTemporary(true);
        var temporaryFilename = dirname.createSubfile(metadata.toString());
        // assert directory path creation
        FutureMkdirCommand.create(handler.getFileManager(), SimpleCDNHandler.sanitazePath(dirname), handler.getIdentity()).get();
        {
            // extract expected file size
            long contentLength;
            var contentLengthHeader = exchange.getRequestHeaders().getFirst("Content-Length");
            if (contentLengthHeader != null) {
                contentLength = Long.parseLong(contentLengthHeader);
            } else {
                // just for safety
                contentLength = Long.MAX_VALUE;
            }
            var is = exchange.getRequestBody();
            var bfsz = BufferManager.getBufferSize();
            var tmp = new byte[bfsz];
            BufferWrapper bufWrapper;
            int len;
            java.nio.ByteBuffer buf;
            boolean success;
            try {
                bufWrapper = BufferManager.getBuffer();
                buf = bufWrapper.get();
                // read until data availables or buffer filled
                while (contentLength > 0 && buf.hasRemaining()) {
                    len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                    if (len == -1) {
                        break;
                    }
                    contentLength -= len;
                    receivedFileSize += len;
                    md.update(tmp, 0, len);
                    buf.put(tmp, 0, len);
                }
                buf.flip();
                // create file locally and start storing it
                QueableCommand queable = QueableCreateCommand.submit(handler.getFileManager(),
                    SimpleCDNHandler.sanitazePath(temporaryFilename),
                    handler.getIdentity(),
                    bufWrapper);
                // store file piece by piece
                while (contentLength > 0) {
                    // take a new buffer
                    bufWrapper = BufferManager.getBuffer();
                    buf = bufWrapper.get();
                    // fill this buffer
                    while (contentLength > 0 && buf.hasRemaining()) {
                        len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                        if (len == -1) {
                            break;
                        }
                        contentLength -= len;
                        receivedFileSize += len;
                        md.update(tmp, 0, len);
                        buf.put(tmp, 0, len);
                    }
                    buf.flip();
                    // append new
                    queable.enqueue(bufWrapper);
                }
                // wait for completion
                success = queable.getFuture().get();
            } catch (IOException e) {
                System.err.println("Received only " + receivedFileSize + "/" + contentLength + " bytes");
                e.printStackTrace();
                success = false;
            }
            if (!success) {
                // delete temporary file
                try {
                    FutureDeleteCommand.delete(
                        handler.getFileManager(),
                        SimpleCDNHandler.sanitazePath(temporaryFilename),
                        handler.getIdentity()).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                throw new RuntimeException("Error while creating file: " + temporaryFilename);
            }
        }
        // rename file with final name - i.e. remove "@tmp"
        metadata.setTemporary(false);
        var finalName = dirname.createSubfile(metadata.toString());
        FutureMoveCommand.move(
            handler.getFileManager(),
            SimpleCDNHandler.sanitazePath(temporaryFilename),
            SimpleCDNHandler.sanitazePath(finalName),
            handler.getIdentity()).get();
        // add save new reference to file as available
        var newNode = handler.getFsStatus().addRegularFileNode(finalName);
        // Compute size
        newNode.setSize(receivedFileSize);
        newNode.setFileHash(md.getAlgorithm(), md.digest());
        var lfi = handler.getFsStatus().addLocalFileInfo(newNode);
        // TODO: delete possible old versions
        // send 201 CREATED
        httpCreated(exchange, "File saved as: " + finalName + "\n");
        // Local files updated!
        handler.updateLastFileUpdateTimestamp();
    }

    private void handleDELETE(HttpExchange exchange) throws Exception {
        // should always target latest available version
        var searchPath = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
        // can only delete last available version, then
        // requestedFile cannot include any "@"
        if (searchPath.contains("@")) {
            httpBadRequest(exchange, "Bad path: " + searchPath);
            return;
        }
        // only owner can accept delete
        if (!handler.testOwnershipOrRedirectToManagement(searchPath, exchange)) {
            // file not available here
            return;
        }
        // file available here?
        var candidateVersion = handler.getFsStatus().getLastSuppliableVersion(searchPath);
        if (candidateVersion == null) {
            // 404 NOT FOUND
            httpNotFound(exchange, "File not found: " + searchPath);
            return;
        } else if (candidateVersion.isDeleted()) {
            httpGone(exchange);
            return;
        }
        // require file deletion
        candidateVersion.markAsDeleted();
        // confirm deletion
        httpOk(exchange, "Deleted file: " + searchPath);
        // Local files updated!
        handler.updateLastFileUpdateTimestamp();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case "GET":
                    // download file - same as for users but include
                    // metadata inside HTTP response headers
                    handleGET(exchange);
                    break;
                case "DELETE":
                    // delete file - handle deletion protocol
                    handleDELETE(exchange);
                    break;
                case "PUT":
                    // upload - handle replication protocol
                    handlePUT(exchange);
                    break;
                default:
                    httpMethodNotAllowed(exchange);
            }
        } catch (Exception e) {
            System.err.println("Error: " + exchange.getRequestMethod() + " " + exchange.getRequestURI().toString());
            e.printStackTrace();
            // handle unexpected error
            httpInternalServerError(exchange);
        }
    }
}
