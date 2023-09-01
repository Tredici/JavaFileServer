package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;

import it.sssupserver.app.base.BufferManager;

public interface QueableCommand {
    public QueableCommand enqueue(BufferManager.BufferWrapper data);
    public CompletableFuture<Boolean> getFuture();
}
