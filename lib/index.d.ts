import { IHeaders } from 'kafkajs';
type MessageCallback = (topic: string, message: string | undefined) => void;
export declare class KafkaService {
    private kafka;
    private logger;
    private admin;
    constructor();
    createTopic(topic: string): Promise<void>;
    sendMessage(topic: string, message: Array<Object> | Object | string, headers?: IHeaders): Promise<import("kafkajs").RecordMetadata[]>;
    consumeMessages(topic: string, callback: MessageCallback): Promise<void>;
}
export {};
