import { IHeaders } from 'kafkajs';
type MessageCallback = (topic: string, message: string | undefined) => void;
export declare class KafkaService {
    private kafka;
    private logger;
    private admin;
    /**
     * Initializes the KafkaService.
     * env variables:
     * KAFKA_BROKERS: comma separated list of kafka brokers
     * KAFKA_CLIENT_ID: client id for kafka
     * KAFKA_REQUEST_TIMEOUT: request timeout for kafka
     * KAFKA_RETRY: number of retries for kafka
     * KAFKA_INITIAL_RETRY_TIME: initial retry time for kafka
     * KAFKA_LOG_LEVEL: log level for kafka
     */
    constructor();
    /**
     * Creates a Kafka topic.
     *
     * @param topic - The name of the topic to create. :: ex => topic=topic1,topic2
     * @returns A promise that resolves when the topic is created.
     */
    createTopic(topic: string): Promise<void>;
    /**
     * Sends a message to a Kafka topic.
     *
     * @param topic - The name of the Kafka topic.
     * @param message - The message to be sent. It can be an array of objects, a single object, or a string.
     * @param headers - Optional headers to be included in the message.
     * @returns A Promise that resolves to the record of the sent message.
     */
    sendMessage(topic: string, message: Array<Object> | Object | string, headers?: IHeaders): Promise<import("kafkajs").RecordMetadata[]>;
    /**
     * Consume messages from a Kafka topic and invoke the provided callback for each message.
     * @param topic - The Kafka topic to consume messages from.
     * @param callback - The callback function to be invoked for each consumed message.
     * @returns A Promise that resolves when the consumption is complete.
     */
    consumeMessages(topic: string, callback: MessageCallback): Promise<void>;
}
export {};
