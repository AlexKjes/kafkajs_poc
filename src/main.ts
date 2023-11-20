import { KafkaContainer} from "@testcontainers/kafka"
import { Consumer, EachMessageHandler, EachMessagePayload, Kafka, Producer } from "kafkajs"

var KAFKA_PORT: number;

async function createKafkaContainer() {
    try {
        const kafkaContainer = await new KafkaContainer().withExposedPorts(9093)
            .start();
        await kafkaContainer.exec("confluent kafka topic create --partitions 1 test-topic-1");
        await kafkaContainer.exec("confluent kafka topic create --partitions 1 test-topic-2");
        console.log(kafkaContainer.getMappedPort(9093));
        KAFKA_PORT = kafkaContainer.getMappedPort(9093);
    } catch(e: any) {
        console.log(e);
    }
}

async function createTopics() {
    let kafka = new Kafka({
        clientId: "admin",
        brokers: [`localhost:${KAFKA_PORT}`]
    });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
        topics: [
            {topic: "test-topic-1", numPartitions: 2},
            {topic: "test-topic-2", numPartitions: 2}
        ]
    })
}

async function kafkaClient(args: {clientName: string, producerTopic: string, consumerTopic:string, consumerGroup: string, starter: boolean}) {

    let kafka = new Kafka({
        clientId: args.clientName,
        brokers: [`localhost:${KAFKA_PORT}`]
    })

    await createConsumer(kafka.consumer({groupId: args.consumerGroup, allowAutoTopicCreation: true, }), args.consumerTopic, async ({topic, partition, message}) => {
        await new Promise(resolve => setTimeout(resolve, 1000))
        let textMessage = message.value?.toString() ?? "Error";
        let messageObject = JSON.parse(textMessage);
        console.log(`Recieved message from ${messageObject.sender}, with counter: ${messageObject.counter}`);
        kafkaSend(kafka.producer(), args.producerTopic, JSON.stringify({
            sender: args.clientName,
            counter: (messageObject.counter ?? 0) + 1
        }))
    })

    if (args.starter) {
        kafkaSend(kafka.producer(), args.producerTopic, JSON.stringify({sender: args.clientName, counter: 1}))
    }

}

async function createConsumer(consumer: Consumer, topic: string, consumeFn: EachMessageHandler) {
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: true})
    await consumer.run( {
        eachMessage: consumeFn
    })
}

async function kafkaSend(producer: Producer, topic: string, message: string) {
    console.log(message);
    
    await producer.connect();
    await producer.send({
        topic: topic,
        messages: [
            { value: message}
        ]
    })
    await producer.disconnect();
}


createKafkaContainer()
    .then(createTopics)
    .then(()=>{
    kafkaClient({ clientName: "testClient1", producerTopic: "test-topic-1", consumerTopic: "test-topic-2", consumerGroup: "group1", starter:false})
    kafkaClient({ clientName: "testClient2", producerTopic: "test-topic-2", consumerTopic: "test-topic-1", consumerGroup: "group2", starter:true})    
    kafkaClient({ clientName: "testClient3", producerTopic: "test-topic-2", consumerTopic: "test-topic-1", consumerGroup: "group2", starter:false})    
    kafkaClient({ clientName: "testClient4", producerTopic: "test-topic-1", consumerTopic: "test-topic-2", consumerGroup: "group1", starter:false})    
})


