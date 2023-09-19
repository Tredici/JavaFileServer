package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;
import java.time.Instant;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import it.sssupserver.app.handlers.simplecdnhandler.RemoteFileInfo;

import static it.sssupserver.app.base.HexUtils.*;

public class RemoteFileInfoGson
    implements JsonDeserializer<RemoteFileInfo> {

    @Override
    public RemoteFileInfo deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var jObj = json.getAsJsonObject();
        // search path
        var searchPath = jObj.get("SearchPath").getAsString();
        // size
        var size = jObj.get("Size").getAsLong();
        // timestamp
        var timestamp = Instant.ofEpochMilli(jObj.get("LastUpdated").getAsLong());
        // deleted
        var deleted = !jObj.has("Deleted") ? false : jObj.get("Deleted").getAsBoolean();
        // hash algorithm
        var hashAlgorithm = jObj.get("HashAlgorithm").getAsString();
        // (content) hash value
        var hash = hexToBytes(jObj.get("Hash").getAsString());

        // Build object to be returned
        var ans = new RemoteFileInfo(searchPath, size, timestamp, hashAlgorithm, hash, deleted);

        return ans;
    }
    
}
