package br.com.dojot;

import org.json.JSONObject;

public class Agent {
    public Integer on_create(JSONObject jsonObject) {
        System.out.println("on create: " + jsonObject.toString());
        return 0;
    }

    public Integer on_update(JSONObject jsonObject) {
        System.out.println("on update: " + jsonObject.toString());
        return 0;
    }

    public Integer on_delete(JSONObject jsonObject) {
        System.out.println("on delete: " + jsonObject.toString());
        return 0;
    }
}
