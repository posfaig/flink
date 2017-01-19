package org.apache.flink.streaming.connectors.mqtt.internal;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageListenerQos0<T> implements IMqttMessageListener {

	private static final Logger LOG = LoggerFactory.getLogger(MessageListenerQos0.class);

	/** The source context to emit records and watermarks to */
	protected final SourceContext<T> sourceContext;

	protected DeserializationSchema<T> schema;

	public MessageListenerQos0(SourceContext<T> sourceContext, DeserializationSchema<T> schema){
		this.sourceContext = checkNotNull(sourceContext);
		this.schema = schema;
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		LOG.debug("messageArrived - topic=" + topic + ", message=" + message);

		synchronized (sourceContext.getCheckpointLock()) {
			T result = schema.deserialize(message.getPayload());
			sourceContext.collect(result);
		}

	}

}
