package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler.ManagedFileSystemStatus;

public class SuppliableFilesWithIdentityGson implements JsonSerializer<SuppliableFilesWithIdentityGson> {
    private DataNodeDescriptor identity;
    private ManagedFileSystemStatus.LocalFileInfo[] localFileInfos;

    public SuppliableFilesWithIdentityGson(DataNodeDescriptor identity, ManagedFileSystemStatus.LocalFileInfo[] localFileInfos) {
        this.identity = identity;
        this.localFileInfos = localFileInfos;
    }

    @Override
    public JsonElement serialize(SuppliableFilesWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.add("Identity", context.serialize(identity));
        jObj.add("SuppliableFiles", context.serialize(localFileInfos));
        return jObj;
    }
}
