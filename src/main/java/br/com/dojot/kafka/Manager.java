package br.com.dojot.kafka;

import br.com.dojot.config.Config;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Manager {
    Logger mLogger = Logger.getLogger(Manager.class);
    private Map<String, Thread> mConsumers;
    private Cache<String, JSONObject> mCache;
    private Map<String, List<Function<JSONObject, Integer>>> mCallbacks;

    public Manager() {
        this.mCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build();

        mCallbacks = new HashMap<>();

        this.initConsumer();
    }

    public void addCallback(String event, Function<JSONObject, Integer> callback) {
        if (!mCallbacks.containsKey(event)) {
            List<Function<JSONObject, Integer>> callbackList = new ArrayList<>();
            callbackList.add(callback);
            mCallbacks.put(event, callbackList);
        } else {
            List<Function<JSONObject, Integer>> callbackList = mCallbacks.get(event);
            callbackList.add(callback);
        }
    }

    private List<String> listTenants() {
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

    private Integer on_tenant_message(String message) {
        mLogger.debug("On tenant message: " + message);
        try {
            JSONObject kafkaEvent = new JSONObject(message);
            JSONObject dataJson = kafkaEvent.getJSONObject("data");
            JSONObject valueJson = dataJson.getJSONObject("value");
            bootstrapTenant(valueJson.get("tenant").toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception);
        }
        return 0;
    }

    private Integer on_tenant_connect(String message) {
        mLogger.debug("On tenant connect");
        if (!this.mConsumers.containsKey("tenancy")) {
            List<String> tenants = listTenants();
            for (String tenant : tenants) {
                bootstrapTenant(tenant);
            }
            mLogger.info("Tenancy context management initialized");
            this.mConsumers.put("tenancy", null);
        } else {
            mLogger.info("Tenancy subscription rebalanced");
        }
        return 0;
    }

    private Integer on_device_connect(String message) {
        mLogger.debug("On device connect");
        mLogger.info("Device consumer ready");
        return 0;
    }

    private Integer on_device_message(String message) {
        mLogger.debug("On device message: " + message);
        try {
            JSONObject kafkaEvent = new JSONObject(message);
            JSONObject dataJson = kafkaEvent.getJSONObject("data");
            String event = kafkaEvent.getString("event");

            JSONObject metaJson = kafkaEvent.getJSONObject("meta");
            String tenant = metaJson.getString("service");

            mLogger.debug("Event received: " + event + " for tenant " + tenant);

            if (!event.equals("template.update")) {
                StringBuilder eventType = new StringBuilder("device.");
                eventType.append(event);

                String deviceId = dataJson.getString("id");

                String key = this.getCacheKey(tenant, deviceId);

                mLogger.debug("Cache key: " + key);
                mLogger.debug("Updating device cache for " + deviceId);

                switch (event) {
                    case "create":
                    case "update":
                        this.mCache.put(key, dataJson);
                        break;
                    case "remove":
                        this.mCache.invalidate(key);
                        break;
                }
            } else {
                JSONArray affected = kafkaEvent.getJSONArray("affected");
                for (int i = 0; i < affected.length(); i++) {
                    String affectedDeviceId = affected.getString(i);
                    mLogger.debug("Updating device cache for " + affectedDeviceId);
                    this.mCache.invalidate(this.getCacheKey(tenant, affectedDeviceId));
                }
            }

            if (mCallbacks.containsKey(event)) {
                List<Function<JSONObject, Integer>> callbackList = mCallbacks.get(event);
                for (Function<JSONObject, Integer> callback : callbackList) {
                    callback.apply(dataJson);
                }
            }
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception);
        }

        return 0;
    }

    private void bootstrapTenant(String tenant) {
        String consumerId = tenant + ".device";
        if (this.mConsumers.containsKey(consumerId)) {
            mLogger.info("Attempted to re-init device consumer for tenant: " + tenant);
        } else {
            Consumer consumer = new Consumer(tenant, Config.getInstance().getDeviceManagerDefaultSubject(), false, null);
            consumer.addCallback("message", this::on_device_message);
            consumer.addCallback("connect", this::on_device_connect);
            Thread thread = new Thread(consumer);
            thread.start();
            mConsumers.put(consumerId, thread);
        }

    }

    private String getCacheKey(String tenant, String deviceId) {
        StringBuilder response = new StringBuilder("device:");
        response.append(tenant);
        response.append(":");
        response.append(deviceId);
        return response.toString();
    }

    private void initConsumer() {
        mConsumers = new HashMap<>();
        Consumer internalConsumer = new Consumer("internal", Config.getInstance().getTenancyManagerDefaultSubject(), true, null);
        internalConsumer.addCallback("message", this::on_tenant_message);
        internalConsumer.addCallback("connect", this::on_tenant_connect);
        Thread thread = new Thread(internalConsumer);
        thread.start();
        mConsumers.put("internal", thread);
    }
}
