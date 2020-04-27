require("dotenv").config()
import { KafkaPubSub } from '../index'
import * as Logger from 'bunyan';
import { ifError } from 'assert';

const kafkaPubSubFactory = (options?) => (): KafkaPubSub =>  
    new KafkaPubSub({
        topic: process.env.KAFKA_TOPIC || 'test',
        host: process.env.KAFKA_HOST || 'localhost',
        port: process.env.KAFKA_PORT || '9092',
        logger: Logger.createLogger({
          name: 'pubsub',
          stream: process.stdout,
          level: 'debug'
        }),
        globalConfig: {
          //'debug': 'all',
          'security.protocol': process.env.KAFKA_SECURITY || 'PLAINTEXT',
        },
        ...options
      })

const testPublishSubscribe = (pubsubFactory: () => KafkaPubSub) =>  
  async () => {
    
    const pubsub = pubsubFactory() 

    const inputChannel = 'test-subscribe'
    const inputPayload = {
      id: 'subscribe-value',
    }
    
    const messagePromise: Promise<number> = new Promise(async (resolve, reject) => {
      const subscription = await pubsub.subscribe(inputChannel, (payload) => {
        try {
          expect(payload).toStrictEqual(inputPayload);
          resolve(subscription);
        } catch (error) {
          reject();
        }
      })
    })
    
    await new Promise(r => setTimeout(r, 5000));
    await pubsub.publish(inputChannel, inputPayload)
    await messagePromise

    console.log("closing")

    await pubsub.close()
  }

const testAsyncIterate = (pubsubFactory: () => KafkaPubSub) =>  
  async () => {

    const pubsub = pubsubFactory() 
      
    const inputChannel = 'test-iterator'
    const iter = pubsub.asyncIterator(inputChannel)
    const iter2 = pubsub.asyncIterator(inputChannel)

    var i: number;
    const rep = 10;

    for (i = 0; i < rep; i++) {
      const inputPayload = { id: "iter-"+i }
      
      const promise = iter.next()
      const promise2 = iter2.next()
      
      if (i === 0) {
        // wait a bit for the subscription to work Kafka
        await new Promise(r => setTimeout(r, 5000));
      }
      
      await pubsub.publish(inputChannel, inputPayload)
      
      expect((await promise).value).toStrictEqual(inputPayload)
      expect((await promise2).value).toStrictEqual(inputPayload)
    }

    await iter.return();
    await iter2.return();

    await pubsub.close()
  }

describe('KafkaPubSub Basic Tests', () => {
  beforeAll(() => {
    jest.setTimeout(60000);
  });
  test('should subscribe and publish messages correctly', testPublishSubscribe(kafkaPubSubFactory()))
  test('should subscribe correctly using asyncIterator', testAsyncIterate(kafkaPubSubFactory()))
})

// describe('KafkaPubSub with Header Tests', () => {
//   beforeAll(() => {
//     jest.setTimeout(60000);
//   });
//   test('should subscribe and publish messages correctly', testPublishSubscribe(kafkaPubSubFactory({useHeaders:true})))
//   test('should subscribe correctly using asyncIterator', testAsyncIterate(kafkaPubSubFactory({useHeaders:true})))
// })