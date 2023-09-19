package it.sssupserver.app.handlers.simplecdnhandler.gson;

import java.lang.reflect.Type;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class SearchPathWithHashGson implements JsonSerializer<SearchPathWithHashGson> {
    private String searchPath;
    private long hash;

    public String getSearchPath() {
        return searchPath;
    }
    public long getHash() {
        return hash;
    }

    public SearchPathWithHashGson(String searchPath, long hash) {
        this.searchPath = searchPath;
        this.hash = hash;
    }

    @Override
    public JsonElement serialize(SearchPathWithHashGson src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("SearchPath", src.searchPath);
        jObj.addProperty("Hash", src.hash);
        return jObj;
    }

    public String toJson() {
        var gson = new GsonBuilder()
            .registerTypeAdapter(SearchPathWithHashGson.class, this)
            .create();
        var ans = gson.toJson(this, SearchPathWithHashGson.class);
        return ans;
    }

    public static String toJson(String searchPath, long hash) {
        var ans = new SearchPathWithHashGson(searchPath, hash);
        return ans.toJson();
    }
}
