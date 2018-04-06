package br.com.dojot.kafka;

import br.com.dojot.config.Config;
import br.com.dojot.utils.Services;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Manager {
    Logger mLogger = Logger.getLogger(Manager.class);
    private Map<String, Thread> mConsumers;
    private Map<String, List<Function<JSONObject, Integer>>> mCallbacks;
    private Producer mProducer;

    public Manager() {
        mCallbacks = new HashMap<>();

        this.initConsumer();
        this.initProducer();
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

    public void updateAttrs(String deviceId, String tenant, JSONObject attrs, JSONObject metadata) {
        if (metadata == null) {
            metadata = new JSONObject();
        }
        this.checkCompleteMetaFields(deviceId, tenant, metadata);
        JSONObject event = new JSONObject();
        event.put("metadata", metadata);
        event.put("attrs", attrs);
        this.mProducer.sendEvent(tenant, Config.getInstance().getIotagentDefaultSubject(), event);
    }

    public void setOnline(String deviceId, String tenant, Long expireAt) {
        if (expireAt == null) {
            expireAt = Instant.now().toEpochMilli();
        }
        JSONObject metadata = new JSONObject();
        this.checkCompleteMetaFields(deviceId, tenant, metadata);

        JSONObject status = new JSONObject();
        status.put("value", "online");
        status.put("expires", expireAt);

        metadata.put("status", status);

        JSONObject event = new JSONObject();
        event.put("metadata", metadata);

        mLogger.debug("Device[" + deviceId + "] for tenant[" + tenant + "] will expire at " + expireAt);

        this.mProducer.sendEvent(tenant, Config.getInstance().getIotagentDefaultSubject(), event);
    }

    public void setOffline(String deviceId, String tenant) {
        this.setOnline(deviceId, tenant, null);
    }

    private void checkCompleteMetaFields(String deviceId, String tenant, JSONObject metadata) {
        if (!metadata.has("deviceid")) {
            metadata.put("deviceid", deviceId);
        }

        if (!metadata.has("tenant")) {
            metadata.put("tenant", tenant);
        }

        if (!metadata.has("timestamp")) {
            Long now = Instant.now().toEpochMilli();
            metadata.put("timestamp", now);
        }

        if (!metadata.has("templates")) {
            JSONObject device = Services.getInstance().getDevice(deviceId, tenant);
            if (device != null) {
                try {
                    metadata.put("templates", device.get("templates"));
                } catch (JSONException exception) {
                    mLogger.error("Json error: " + exception);
                }
            } else {
                mLogger.error("Cannot get templates for deviceId: " + deviceId);
            }
        }
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
            List<String> tenants = Services.getInstance().listTenants();
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

                String key = Services.getInstance().getCacheKey(tenant, deviceId);

                mLogger.debug("Cache key: " + key);
                mLogger.debug("Updating device cache for " + deviceId);

                switch (event) {
                    case "create":
                    case "update":
                        Services.getInstance().addDeviceToCache(key, dataJson);
                        break;
                    case "remove":
                        Services.getInstance().removeDeviceFromCache(key);
                        break;
                }
            } else {
                JSONArray affected = kafkaEvent.getJSONArray("affected");
                for (int i = 0; i < affected.length(); i++) {
                    String affectedDeviceId = affected.getString(i);
                    mLogger.debug("Updating device cache for " + affectedDeviceId);
                    Services.getInstance().removeDeviceFromCache(Services.getInstance().getCacheKey(tenant, affectedDeviceId));
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

    private void initConsumer() {
        mConsumers = new HashMap<>();
        Consumer internalConsumer = new Consumer("internal", Config.getInstance().getTenancyManagerDefaultSubject(), true, null);
        internalConsumer.addCallback("message", this::on_tenant_message);
        internalConsumer.addCallback("connect", this::on_tenant_connect);
        Thread thread = new Thread(internalConsumer);
        thread.start();
        mConsumers.put("internal", thread);
    }

    private void initProducer() {
        this.mProducer = new Producer(null);
    }
}
