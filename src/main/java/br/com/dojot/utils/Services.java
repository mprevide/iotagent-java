package br.com.dojot.utils;

import br.com.dojot.auth.Auth;
import br.com.dojot.config.Config;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Services {
    Logger mLogger = Logger.getLogger(Services.class);
    private static Services mInstance;
    private Cache<String, JSONObject> mCache;

    private Services() {
        this.mCache = Caffeine.newBuilder()
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build();
    }

    public static synchronized Services getInstance() {
        if (mInstance == null) {
            mInstance = new Services();
        }
        return mInstance;
    }

    public synchronized void addDeviceToCache(String key, JSONObject data) {
        this.mCache.put(key, data);
    }

    public synchronized void removeDeviceFromCache(String key) {
        this.mCache.invalidate(key);
    }

    public List<String> listTenants() {
        List<String> tenants = new ArrayList<>();

        StringBuilder url = new StringBuilder(Config.getInstance().getTenancyManagerDefaultManager());
        url.append("/admin/tenants");
        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString()).asJson();
            JSONObject jsonResponse = request.getBody().getObject();
            JSONArray jsonArray = jsonResponse.getJSONArray("tenants");
            for (int i = 0; i < jsonArray.length(); i++) {
                tenants.add(jsonArray.getString(i));
            }
        } catch (UnirestException exception) {
            mLogger.error("Cannot get url[" + url.toString() + "]");
            mLogger.error("Failed to acquire existing tenancy contexts");
            mLogger.error("Error: " + exception.toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception.toString());
        }

        return tenants;
    }

    public List<String> listDevices(String tenant) {
        List<String> devices = new ArrayList<>();

        StringBuilder url = new StringBuilder(Config.getInstance().getDeviceManagerDefaultManager());
        url.append("/device?idsOnly");
        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString())
                    .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                    .asJson();

            JSONArray jsonResponse = request.getBody().getArray();
            for (int i = 0; i < jsonResponse.length(); i++) {
                devices.add(jsonResponse.getString(i));
            }
        } catch (UnirestException exception) {
            mLogger.error("Cannot get url[" + url.toString() + "]");
            mLogger.error("Failed to acquire existing devices");
            mLogger.error("Error: " + exception.toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception.toString());
        }

        return devices;
    }

    public JSONObject getDevice(String deviceId, String tenant) {
        String key = this.getCacheKey(tenant, deviceId);

        JSONObject cached = this.mCache.getIfPresent(key);
        if (cached != null) {
            mLogger.debug("Device [" + deviceId + "] is already cached for tenant " + tenant);
            return cached;
        }

        StringBuilder url = new StringBuilder(Config.getInstance().getDeviceManagerDefaultManager());
        url.append("/internal/device/");
        url.append(deviceId);
        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString())
                    .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                    .asJson();
            JSONObject deviceResponse = request.getBody().getObject();
            this.mCache.put(key, deviceResponse);
            return deviceResponse;
        } catch (UnirestException exception) {
            mLogger.error("Cannot get url[" + url.toString() + "]");
            mLogger.error("Failed to acquire existing tenancy contexts");
            mLogger.error("Error: " + exception.toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception.toString());
        }

        return null;
    }

    public String getCacheKey(String tenant, String deviceId) {
        StringBuilder response = new StringBuilder("device:");
        response.append(tenant);
        response.append(":");
        response.append(deviceId);
        return response.toString();
    }
}
