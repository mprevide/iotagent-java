package br.com.dojot;

import br.com.dojot.IoTAgent.Manager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class IoTAgentExample {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(IoTAgentExample.class);
        Agent agent = new Agent();

        Manager manager = new Manager();

        manager.addCallback("create", agent::on_create);
        manager.addCallback("update", agent::on_update);
        manager.addCallback("delete", agent::on_delete);

        while (true) {
            logger.info("Running IoTAgent Example");

            JSONObject eventData = new JSONObject();

            eventData.put("param_1", "valor_1");
            eventData.put("param_2", "valor_2");
            eventData.put("param_3", "valor_3");

            logger.info(eventData.toString());

            manager.updateAttrs("8743ba", "admin", eventData, null);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException exception) {
                logger.error("Exception: " + exception.toString());
            }
        }
    }
}
