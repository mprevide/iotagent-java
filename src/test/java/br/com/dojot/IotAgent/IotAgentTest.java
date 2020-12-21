package br.com.dojot.IotAgent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.cpqd.app.config.Config;
import com.cpqd.app.messenger.Messenger;

import br.com.dojot.IoTAgent.IoTAgent;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ IoTAgent.class })
public class IotAgentTest {

	@Before
	public void setup() {
		PowerMockito.mockStatic(IoTAgent.class);
	}

	@Test
	public void ShouldPublishMessageWithCustomStatus() throws Exception {

		// given

		String deviceId = "e097bd";
		String tenant = "admin";

		Messenger messengerMock = mock(Messenger.class);
		long kafkaDefaultConsumerPollTime = Config.getInstance().getKafkaDefaultConsumerPollTime();

		JSONObject customStatus = new JSONObject();
		customStatus.put("custom attribute", "sample value");
		customStatus.put("another custom attribute", "sample value");

		// when

		when(IoTAgent.getMessenger(kafkaDefaultConsumerPollTime)).thenReturn(messengerMock);

		// then

		new IoTAgent(kafkaDefaultConsumerPollTime).publishStatus(deviceId, tenant, customStatus);

		ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> tenantCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

		verify(messengerMock, times(1)).publish(subjectCaptor.capture(), tenantCaptor.capture(),
				messageCaptor.capture());

		assertEquals("subject does not match", "device-data", subjectCaptor.getValue());
		assertEquals("tenant does not match", tenant, tenantCaptor.getValue());
		JSONObject message = new JSONObject(messageCaptor.getValue());
		assertEquals("sample value", message.getJSONObject("metadata").getJSONObject("status").get("custom attribute"));
		assertEquals("sample value",
				message.getJSONObject("metadata").getJSONObject("status").get("another custom attribute"));

	}

	@Test
	public void ShouldPublishWithSubjectPredefined() throws Exception {

		// given

		String deviceId = "e097bd";
		String tenant = "admin";

		Messenger messengerMock = mock(Messenger.class);
		long kafkaDefaultConsumerPollTime = Config.getInstance().getKafkaDefaultConsumerPollTime();

		// when

		when(IoTAgent.getMessenger(kafkaDefaultConsumerPollTime)).thenReturn(messengerMock);

		// then

		new IoTAgent(kafkaDefaultConsumerPollTime).publishStatus(deviceId, tenant, null);

		ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> tenantCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

		verify(messengerMock, times(1)).publish(subjectCaptor.capture(), tenantCaptor.capture(),
				messageCaptor.capture());

		assertEquals("subject does not match", "device-data", subjectCaptor.getValue());

	}

	@Test
	public void ShouldPublishWithInformedTenant() throws Exception {

		// given

		String deviceId = "e097bd";
		String tenant = "admin";

		Messenger messengerMock = mock(Messenger.class);
		long kafkaDefaultConsumerPollTime = Config.getInstance().getKafkaDefaultConsumerPollTime();

		// when

		when(IoTAgent.getMessenger(kafkaDefaultConsumerPollTime)).thenReturn(messengerMock);

		// then

		new IoTAgent(kafkaDefaultConsumerPollTime).publishStatus(deviceId, tenant, null);

		ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> tenantCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

		verify(messengerMock, times(1)).publish(subjectCaptor.capture(), tenantCaptor.capture(),
				messageCaptor.capture());

		assertEquals("tenant does not match", "admin", tenantCaptor.getValue());

	}

}
