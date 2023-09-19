package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.base.FileTree;
import it.sssupserver.app.handlers.simplecdnhandler.DataNodeDescriptor;

public class FileInfoWithIdentityGson implements JsonSerializer<FileInfoWithIdentityGson> {
    private DataNodeDescriptor identity;
    private FileTree.Node[] nodes;

    public FileInfoWithIdentityGson(DataNodeDescriptor identity, FileTree.Node[] nodes) {
        this.identity = identity;
        this.nodes = nodes;
    }

    @Override
    public JsonElement serialize(FileInfoWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.add("Identity", context.serialize(identity));
        jObj.add("LocalFiles", context.serialize(nodes));
        return jObj;
    }
}
