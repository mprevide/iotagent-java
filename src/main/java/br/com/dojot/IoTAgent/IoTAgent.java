package br.com.dojot.IoTAgent;

import org.json.JSONObject;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.function.BiFunction;

import com.cpqd.app.messenger.Messenger;
import com.cpqd.app.config.Config;


public class IoTAgent {
    private Logger mLogger = Logger.getLogger(IoTAgent.class);
    private Messenger mMessenger;

    public IoTAgent(Long consumerPollTime) {
        this.mMessenger = new Messenger(consumerPollTime);
        mLogger.info("Initializing Messenger for Iotagent-java...");
        this.mMessenger.init();
        mLogger.info("... Messenger was successfully initialized.");
        mLogger.info("creating channel...");
        this.mMessenger.createChannel(Config.getInstance().getIotagentDefaultSubject(),"w",false);
        this.mMessenger.createChannel(Config.getInstance().getDeviceManagerDefaultSubject(), "rw", false);
        this.mMessenger.on(Config.getInstance().getDeviceManagerDefaultSubject(), "message", (tenant, msg) -> {
            this.callback(tenant, msg);
            return null;
        });
    }

    public void generateDeviceCreateEventForActiveDevices(){
        this.mMessenger.generateDeviceCreateEventForActiveDevices();
    }

    public void callback(String tenant, String message) {
        JSONObject messageObj = new JSONObject(message);

        String eventType = "device." + messageObj.get("event").toString();
        this.mLogger.debug(messageObj.toString());
        this.mMessenger.emit("iotagent.device", tenant, eventType, messageObj.toString());
    }

    public void updateAttrs(String deviceId, String tenant, JSONObject attrs, JSONObject metadata) {
        if (metadata == null) {
            metadata = new JSONObject();
        }
        this.checkCompleteMetaFields(deviceId, tenant, metadata);
        JSONObject event = new JSONObject();
        event.put("metadata", metadata);
        event.put("attrs", attrs);
        this.mMessenger.publish(Config.getInstance().getIotagentDefaultSubject(), tenant, event.toString());
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
    }


    public void on (String subject, String event, BiFunction<String, String, Void> callback){
        this.mMessenger.createChannel(subject, "r", false );
        this.mMessenger.on(subject, event, callback);
    }

}
