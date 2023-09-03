package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import it.sssupserver.app.base.FileTree.Node;
import static it.sssupserver.app.base.HexUtils.*;

class NodeGson implements JsonSerializer<Node> {
    @Override
    public JsonElement serialize(Node src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("Path", src.getPath().toString());
        jObj.addProperty("Size", src.getSize());
        jObj.addProperty("HashAlgorithm", src.getHashAlgorithm());
        jObj.addProperty("Hash", bytesToHex(src.getFileHash()));
        return jObj;
    }
}
