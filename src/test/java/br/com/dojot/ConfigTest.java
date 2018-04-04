package br.com.dojot;

import br.com.dojot.config.Config;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.junit.Assert.assertEquals;

public class ConfigTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void testGetDeviceManagerAddressNoEnv() {
        String deviceManagerAddress = Config.getInstance().getDeviceManagerAddress();
        assertEquals(deviceManagerAddress, "device-manager:5000");
    }

    @Test
    public void testGetDeviceManagerAddressEnvSet() {
        environmentVariables.set("DEVM_ADDRESS", "device-manager-address-test");
        assertEquals(System.getenv("DEVM_ADDRESS"), "device-manager-address-test");
    }

    @Test
    public void testGetAuthAddressNoEnv() {
        String deviceManagerAddress = Config.getInstance().getAuthAddress();
        assertEquals(deviceManagerAddress, "auth:5000");
    }

    @Test
    public void testGetAuthAddressEnvSet() {
        environmentVariables.set("AUTH_ADDRESS", "auth-address-test");
        assertEquals(System.getenv("AUTH_ADDRESS"), "auth-address-test");
    }

    @Test
    public void testGetDataBrokerAddressNoEnv() {
        String deviceManagerAddress = Config.getInstance().getDataBrokerAddress();
        assertEquals(deviceManagerAddress, "data-broker:80");
    }

    @Test
    public void testGetDataBrokerAddressEnvSet() {
        environmentVariables.set("DATA_BROKER_ADDRESS", "data-broker-address-test");
        assertEquals(System.getenv("DATA_BROKER_ADDRESS"), "data-broker-address-test");
    }

    @Test
    public void testGetKafkaAddressNoEnv() {
        String deviceManagerAddress = Config.getInstance().getKafkaAddress();
        assertEquals(deviceManagerAddress, "kafka:9092");
    }

    @Test
    public void testGetKafkaAddressEnvSet() {
        environmentVariables.set("KAFKA_ADDRESS", "kafka-address-test");
        assertEquals(System.getenv("KAFKA_ADDRESS"), "kafka-address-test");
    }
}
