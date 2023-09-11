package it.sssupserver.app.handlers.simplecdnhandler;

import java.lang.reflect.Type;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * Used to parse response from /api/suppliables
 *
 * Paired with SuppliableFilesWithIdentityGson
 */
public class RemoteFilesWithIdentityGson
    implements JsonDeserializer<RemoteFilesWithIdentityGson> {

    private DataNodeDescriptor identity;
    private RemoteFileInfo[] files;

    public DataNodeDescriptor getIdentity() {
        return identity;
    }

    public RemoteFileInfo[] getFiles() {
        return files;
    }

    private RemoteFilesWithIdentityGson() {}

    @Override
    public RemoteFilesWithIdentityGson deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var ans = new RemoteFilesWithIdentityGson();
        var jObj = json.getAsJsonObject();
        ans.identity = context.deserialize(jObj.getAsJsonObject("Identity"), DataNodeDescriptor.class);
        ans.files = context.deserialize(jObj.getAsJsonArray("SuppliableFiles"), RemoteFileInfo.class);
        return ans;
    }

    public static RemoteFilesWithIdentityGson parse(String json) {
        var gson = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(RemoteFileInfo.class, new RemoteFileInfoGson())
            .registerTypeAdapter(RemoteFilesWithIdentityGson.class, new RemoteFilesWithIdentityGson())
            .create();
        var ans = gson.fromJson(json, RemoteFilesWithIdentityGson.class);
        return ans;
    }
}
