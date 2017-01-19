package org.apache.flink.streaming.connectors.mqtt;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.mqtt.internal.MessageListenerQos0;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for Flink MQTT Subscriber data sources.
 * Uses the Eclipse Paho MQTT client library.
 * Currently does not perform checkpointing, and uses QoS 0 in the MQTT subscription by default. 
 * <b>NOTE:</b> Currently using QoS 1 or QoS 2 in the MQTT subscription does not provide at-least-once or exactly-once guarantees in Flink with this source class.
 * <b>NOTE:</b> This source has a parallelism of {@code 1}.
 * TODO: Checkpointing and at-least-once/exactly-once guarantees.
 * 
 * @param <T> The type of records produced by this data source
 */
public class MqttSourceQos0<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T>{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(MqttSourceQos0.class);

	// Keys for SSL options of MQTT connection, see MqttConnectOptions class in Eclipse Paho docs
	public static final String KEYSTORE_PATH = "com.ibm.ssl.keyStore";
	public static final String TRUSTSTORE_PATH = "com.ibm.ssl.trustStore";
	public static final String KEYSTORE_PASSWORD = "com.ibm.ssl.keyStorePassword";
	public static final String TRUSTSTORE_PASSWORD = "com.ibm.ssl.trustStorePassword";

	// Keys for MQTT Connection and Client Options
	public static final String MQTT_OPT_CLEAN_SESSION = "mqtt.opt.clean.session";
	public static final String MQTT_OPT_USER_NAME = "mqtt.opt.user.name";
	public static final String MQTT_OPT_PASSWORD = "mqtt.opt.password";
	public static final String MQTT_OPT_AUTOMATIC_RECONNECT = "mqtt.opt.automatic.reconnect";
	public static final String MQTT_OPT_CONNECTION_TIMEOUT = "mqtt.opt.connection.timeout";
	public static final String MQTT_OPT_MAX_INFLIGHT = "mqtt.opt.max.inflight";
	public static final String MQTT_CLIENT_TIME_TO_WAIT = "mqtt.client.time.to.wait";
	public static final String MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT = "mqtt.client.time.to.wait.for.disconnect";
	public static final String MQTT_CLIENT_SUBSCRIPTION_QOS = "mqtt.client.subscription.qos";
	
	// Default values
	public static final long MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT_DEFAULT = 10000L;
	private static final int MQTT_QOS_DEFAULT = 0;
	
	protected DeserializationSchema<T> schema;

	/** User-supplied MQTT connection options for Eclipse Paho MQTT client. See Eclipse Paho docs. **/
	protected final Map<String, Object> mqttSettings;

	/** User-supplied MQTT connection SSL options for Eclipse Paho MQTT client. See Eclipse Paho docs. **/
	protected final Properties sslProperties;

	protected final String serverURI;

	protected final String clientId;

	protected final String topic;

	protected transient MqttClient client;

	/** Flag indicating whether the consumer is still running **/
	private transient volatile boolean running = true;

	/** Callback for Mqtt connection events. **/
	private transient MqttCallback callback = new MqttCallback(){

		@Override
		public void connectionLost(java.lang.Throwable cause){
			LOG.debug("connectionLost - cause=" + cause);
		}

		@Override
		public void deliveryComplete(IMqttDeliveryToken token){
			LOG.debug("deliveryComplete - token=" + token);
		}

		@Override
		public void messageArrived(java.lang.String topic, MqttMessage message){
			LOG.debug("messageArrived - topic=" + topic + ", message=" + message);
		}
	};

	/**
	 * Creates a new MQTT subscriber source.
	 *
	 * @param serverURI
	 *           The server URI of the MQTT broker to connect to. E.g. tcp://localhost:1883 or ssl://localhost:8883. See the documentation of MqttClient in the Eclipse Paho docs. 
	 * @param clientId
	 *           The client id of the subscriber in the MQTT connection. The specified client id will remain unaltered only if the parallelism of the source is set to 1.
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param connectOpts
	 *           The options used to configure the connection of the Eclipse Paho MQTT client.
	 */
	public MqttSourceQos0(String serverURI, 
			String clientId, 
			String topic, 
			Map<String, Object> mqttSettings, 
			Properties sslProperties, 
			DeserializationSchema<T> deserializationSchema) {
		this.serverURI = serverURI;
		this.clientId = clientId;
		this.topic = topic;
		this.mqttSettings = mqttSettings;
		this.sslProperties = sslProperties;
		this.schema = deserializationSchema;
	}

	@Override
	public void cancel() {
		LOG.debug("cancel");

		// set ourselves as not running
		running = false;

		// abort the client, if there is one
		if (client != null){
			if (client.isConnected()){
				try {
					LOG.debug("Disconnecting from MQTT broker");	
					client.disconnect((mqttSettings.get(MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT) == null) ? MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT_DEFAULT : (Long)mqttSettings.get(MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT));
					LOG.debug("Disconnected from MQTT broker");
				} catch (MqttException e){
					LOG.debug("Error while trying to disconnect MQTT client: " + e);
					try {
						client.disconnectForcibly((mqttSettings.get(MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT) == null) ? MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT_DEFAULT : (Long)mqttSettings.get(MQTT_CLIENT_TIME_TO_WAIT_FOR_DISCONNECT));
					} catch (MqttException e2){
						LOG.error("Error while trying to forcibly disconnect MQTT client: " + e);
					}
				}
			}
			try {
				client.close();
			} catch (MqttException e){
				LOG.error("Error while trying to close MQTT client: " + e);
			}
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		LOG.debug("run");

		if (topic == null || topic.isEmpty()) {
			throw new Exception("The topics were not set for the subscriber.");
		}

		LOG.debug("topic=" + topic);
		client.subscribe(topic, 
				((mqttSettings.get(MqttSourceQos0.MQTT_CLIENT_SUBSCRIPTION_QOS) == null) ? MQTT_QOS_DEFAULT : (Integer)mqttSettings.get(MqttSourceQos0.MQTT_CLIENT_SUBSCRIPTION_QOS)), 
				new MessageListenerQos0<T>(sourceContext, schema));

		// since incoming MQTT messages are consumed by an async messagelistener, we block the call here

		// this source never completes, so emit a Long.MAX_VALUE watermark
		// to not block watermark forwarding
		sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

		// wait until this is canceled
		final Object waitLock = new Object();
		while (running) {
			try {
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (waitLock) {
					waitLock.wait();
				}
			}
			catch (InterruptedException e) {
				LOG.debug("InterruptedException - running=" + running);
				if (!running) {
					// restore the interrupted state, and fall through the loop
					Thread.currentThread().interrupt();
				}
			}
		}

	}

	@Override
	public void open(Configuration configuration) {
		LOG.debug("open");
		try {
			// TODO: persistence is not needed since QoS is 0, so we set it to null
			client = new MqttClient(serverURI, clientId, null);
			
			if (mqttSettings.containsKey(MQTT_CLIENT_TIME_TO_WAIT)){
				client.setTimeToWait((Long)mqttSettings.get(MQTT_CLIENT_TIME_TO_WAIT));
			}
			

			LOG.debug("Connecting to broker: " + serverURI + "with clientId=" + clientId);
			client.connect(this.getMqttConnectOptionsFromProperties());
			LOG.debug("Connected to broker:" + serverURI);

			client.setCallback(callback);
		} catch (MqttException e) {
			throw new RuntimeException("Cannot create MQTT connection with clientId=" + clientId + " at " + serverURI, e);
		}
		running = true;
	}

	@Override
	public void close() throws Exception {
		LOG.debug("close");

		// pretty much the same logic as cancelling
		try {
			cancel();
		} finally {
			super.close();
		}
	}

	// ------------------------------------------------------------------------
	//  Helper methods
	// ------------------------------------------------------------------------

	/*
	 * Creates a MqttConnectOptions object from the connection properties object. 
	 * */
	private MqttConnectOptions getMqttConnectOptionsFromProperties(){
		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setSSLProperties(sslProperties);

		if (mqttSettings.containsKey(MQTT_OPT_CLEAN_SESSION)){
			connOpts.setCleanSession((Boolean)mqttSettings.get(MQTT_OPT_CLEAN_SESSION));
		}

		if (mqttSettings.containsKey(MQTT_OPT_USER_NAME)){
			connOpts.setUserName((String)mqttSettings.get(MQTT_OPT_USER_NAME));
		}
		if (mqttSettings.containsKey(MQTT_OPT_PASSWORD)){
			connOpts.setPassword(((String)mqttSettings.get(MQTT_OPT_PASSWORD)).toCharArray());
		}
		if (mqttSettings.containsKey(MQTT_OPT_AUTOMATIC_RECONNECT)){
			connOpts.setAutomaticReconnect((Boolean)mqttSettings.get(MQTT_OPT_AUTOMATIC_RECONNECT));
		}
		if (mqttSettings.containsKey(MQTT_OPT_CONNECTION_TIMEOUT)){
			connOpts.setConnectionTimeout((Integer)mqttSettings.get(MQTT_OPT_CONNECTION_TIMEOUT));
		}
		if (mqttSettings.containsKey(MQTT_OPT_MAX_INFLIGHT)){
			connOpts.setMaxInflight((Integer)mqttSettings.get(MQTT_OPT_MAX_INFLIGHT));
		}
		return connOpts;
	}

	// ------------------------------------------------------------------------
	//  ResultTypeQueryable methods 
	// ------------------------------------------------------------------------

	@Override
	public TypeInformation<T> getProducedType() {
		return schema.getProducedType();
	}
}
