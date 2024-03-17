import { Kafka, Message, Logger, Admin, IHeaders, ITopicConfig } from 'kafkajs'

type MessageCallback = (topic: string, message: string | undefined) => void

export class KafkaService {
  private kafka: Kafka
  private logger: Logger
  private admin: Admin

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
  constructor() {
    const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092']
    const clientId = process.env.KAFKA_CLIENT_ID ?? 'my-app'
    const requestTimeout = process.env?.KAFKA_REQUEST_TIMEOUT ?? 30000
    const retry = process.env?.KAFKA_RETRY ?? 8
    const initialRetryTime = process.env?.KAFKA_INITIAL_RETRY_TIME ?? 100
    const logLevel = process.env?.KAFKA_LOG_LEVEL ?? 0
    this.kafka = new Kafka({
      clientId,
      brokers,
      requestTimeout: Number(requestTimeout),
      retry: {
        initialRetryTime: Number(initialRetryTime),
        retries: Number(retry),
      },
    })
    this.logger = this.kafka.logger()
    this.admin = this.kafka.admin()

    this.logger.info('KafkaService initialized', {
      brokers,
      clientId,
      requestTimeout,
      retry,
      initialRetryTime,
      logLevel,
    })
  }

  /**
   * Creates a Kafka topic.
   *
   * @param topic - The name of the topic to create. :: ex => topic=topic1,topic2
   * @returns A promise that resolves when the topic is created.
   */
  async createTopic(topic: string) {
    // const topics: ITopicConfig[] = topic.split(',').map((topic) => ({ topic }))
    const topics = topic.split(',')

    await this.admin.connect()

    const listTopic = await this.admin.listTopics()
    const existingTopics = listTopic.filter((t) => topics.includes(t))
    const newTopics = topics.filter((t) => !existingTopics.includes(t))

    if (newTopics.length) {
      await this.admin.createTopics({
        topics: newTopics.map((topic) => ({
          topic,
          numPartitions: 1,
        })),
      })
    }
    this.logger.info(`Created topic`, {
      topics: newTopics,
    })
    await this.admin.disconnect()
  }

  /**
   * Sends a message to a Kafka topic.
   *
   * @param topic - The name of the Kafka topic.
   * @param message - The message to be sent. It can be an array of objects, a single object, or a string.
   * @param headers - Optional headers to be included in the message.
   * @returns A Promise that resolves to the record of the sent message.
   */
  async sendMessage(topic: string, message: Array<Object> | Object | string, headers?: IHeaders) {
    if (!headers) {
      headers = {}
    }
    let messages: Message[] = []
    if (typeof message === 'object') {
      if (Array.isArray(message)) {
        message.forEach((msg) => {
          messages.push({ headers, value: JSON.stringify(msg) })
        })
      } else {
        messages.push({ headers, value: JSON.stringify(message) })
      }
    } else {
      messages.push({ headers, value: message })
    }

    const producer = this.kafka.producer()
    await producer.connect()
    const record = await producer.send({ topic, messages })

    this.logger.info(`Sent successfully topic:${topic}`, { ...record })
    await producer.disconnect()
    return record
  }

  /**
   * Consume messages from a Kafka topic and invoke the provided callback for each message.
   * @param topic - The Kafka topic to consume messages from.
   * @param callback - The callback function to be invoked for each consumed message.
   * @returns A Promise that resolves when the consumption is complete.
   */
  async consumeMessages(topic: string, callback: MessageCallback) {
    const consumer = this.kafka.consumer({ groupId: 'test-group' })
    await consumer.connect()
    await consumer.subscribe({ topics: topic.split(','), fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const { headers, offset, value } = message
        const payload = value?.toString() ?? ''

        for (const key in headers) {
          if (headers?.hasOwnProperty(key) && Buffer.isBuffer(headers[key])) {
            headers[key] = headers[key]?.toString()
          }
        }
        this.logger.info('received message', {
          topic,
          partition,
          offset,
          value: payload,
          headers: headers,
        })
        callback(topic, payload)
      },
    })
  }
}
