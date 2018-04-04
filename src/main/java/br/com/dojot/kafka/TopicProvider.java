package br.com.dojot.kafka;

import br.com.dojot.auth.Auth;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TopicProvider {
    Logger mLogger = LoggerFactory.getLogger(TopicProvider.class);
    private Map<String,String> mTopics;
    private static TopicProvider mInstance;

    private TopicProvider() {
        this.mTopics = new HashMap<>();
    }

    public static synchronized TopicProvider getInstance() {
        if (mInstance == null) {
            mInstance = new TopicProvider();
        }
        return mInstance;
    }

    public String getTopic(String subject, String tenant, String broker, Boolean global) {
        StringBuilder key = new StringBuilder(tenant);
        key.append(":");
        key.append(subject);

        if (this.mTopics.containsKey(key.toString())) {
            return this.mTopics.get(key.toString());
        } else {
            StringBuilder url = new StringBuilder(broker);
            url.append("/topic/");
            url.append(subject);
            url.append(global ? "?global=true" : "");
            try {
                HttpResponse<JsonNode> request = Unirest.get(url.toString())
                        .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                        .asJson();
                JSONObject jsonResponse = request.getBody().getObject();
                return jsonResponse.getString("topic");
            } catch (UnirestException exception) {
                mLogger.error("Cannot get url[{}] for tenant[{}]", url, tenant);
                return "";
            } catch (JSONException exception) {
                mLogger.error("Json error: {}", exception);
                return "";
            }
        }
    }
}
