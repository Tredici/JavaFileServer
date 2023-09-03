package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import static it.sssupserver.app.base.HexUtils.*;


/**
 * Gson serializer for LocalFileInfo.Version
 * When serializing, present only the newest version,
 * others are only for internal usage
 */
public class LocalFileVersionGson implements JsonSerializer<SimpleCDNHandler.ManagedFileSystemStatus.LocalFileInfo> {
    private boolean detailed = false;

    public LocalFileVersionGson() {
    }

    public LocalFileVersionGson(boolean printDetailed) {
        detailed = printDetailed;
    }

    @Override
    public JsonElement serialize(SimpleCDNHandler.ManagedFileSystemStatus.LocalFileInfo src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("SearchPath", src.getSearchPath());
        if (!detailed) {
            var v = src.getLastSuppliableVersion();
            jObj.addProperty("Size", v.getSize());
            jObj.addProperty("HashAlgorithm", v.getHashAlgotrithm());
            jObj.addProperty("Hash", bytesToHex(v.getFileHash()));
            jObj.addProperty("LastUpdated", v.getLastUpdateTimestamp().toEpochMilli());
        } else {
            var versions = src.getAllVersions();
            var jArray = new JsonArray(versions.length);
            for (var v : versions) {
                var vObj = new JsonObject();
                vObj.addProperty("Size", v.getSize());
                vObj.addProperty("HashAlgorithm", v.getHashAlgotrithm());
                vObj.addProperty("Hash", bytesToHex(v.getFileHash()));
                vObj.addProperty("LastUpdated", v.getLastUpdateTimestamp().toEpochMilli());
                vObj.addProperty("RealPath", v.getPath().toString());
                jArray.add(vObj);
            }
            jObj.add("Versions", jArray);
        }
        return jObj;
    }
}

