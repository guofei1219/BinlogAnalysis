package canal;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

public class KafkaProducer{
	String topic;
	public KafkaProducer(String topics){
		super();
		this.topic=topics;
	}

	public static void sendMsg(String topic, String sendKey, String data){
		Producer producer = createProducer();
		producer.send(new KeyedMessage<String, String>(topic,sendKey,data));
		//producer.send(new KeyedMessage<String, String>("canal","123","guofei"));
		System.out.println("sendKey:"+sendKey);
	}

	public static Producer<Integer,String> createProducer(){
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "rmhadoop01:2181");
		properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "rmhadoop01:9092");// 声明kafka broker
        return new Producer<Integer,String>(new ProducerConfig(properties));
	}

	public static void main(String args[]) {
		Producer producer = createProducer();
		//producer.send(new KeyedMessage<String, String>(topic,sendKey,data));


		for (int i=1;i<=5;i++){
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(new KeyedMessage<String, String>("canal","123"+i,"guofei\t"+i));
		}

	}
}
