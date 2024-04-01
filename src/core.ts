// import util from 'node:util';
// import parseDuration from 'parse-duration';
// import bytes from 'bytes';
// import {
//   type NatsConnection,
//   type JetStreamManager,
//   connect,
//   RetentionPolicy,
//   StorageType,
//   DiscardPolicy,
//   AckPolicy,
//   DeliverPolicy,
//   ReplayPolicy,
// } from 'nats';
// import {
//   type StreamInfo,
//   type ConsumerInfo,
//   type SubscriptionInfo,
//   NatsClient,
//   System,
// } from './sdk';

// util.inspect.defaultOptions.depth = 20;

// type CoreStreamParams = {
//   name: string;
// };

// type CoreConsumerParams = {
//   stream: string;
//   name: string;
// };

// type CoreOptions = {
//   nats: NatsClient;
// };

// class Core {
//   private _nats: NatsClient;
//   private _jsmInstance: JetStreamManager | undefined;
  
//   constructor(options: CoreOptions) {
//     const {
//       nats,
//     } = options;
    
//     this._nats = nats;
//   }
  
//   private async _jsm(): Promise<JetStreamManager> {
//     if (!this._jsmInstance) {
//       this._jsmInstance = await this._nats.backend.jetstreamManager();
//     }
    
//     return this._jsmInstance;
//   }
  
//   public async stream(params: CoreStreamParams): Promise<StreamInfo> {
//     const jsm = await this._jsm();
    
//     const streamInfo = await jsm.streams.add({
//       name: params.name.replaceAll('.', '--'),
//       retention: RetentionPolicy.Interest,
//       storage: StorageType.File,
//       subjects: [params.name],
//       max_msgs: 100_000,
//       max_age: parseDuration('2h', 'ns'),
//       max_bytes: bytes.parse('100mb'),
//       max_msg_size: bytes.parse('100kb'),
//       discard: DiscardPolicy.Old,
//       // num_replicas: 3,
//     });
    
//     return {
//       name: streamInfo.config.name,
//     };
//   }
  
//   public async consumer(params: CoreConsumerParams): Promise<ConsumerInfo> {
//     const jsm = await this._jsm();
    
//     const consumerInfo = await jsm.consumers.add(params.stream, {
//       ack_policy: AckPolicy.Explicit,
//       deliver_policy: DeliverPolicy.New,
//       durable_name: params.name.replaceAll('.', '--'),
//     });
    
//     return {
//       stream: consumerInfo.stream_name,
//       name: consumerInfo.name,
//     };
//   }
// }

// function validate<T>(value: unknown): value is T {
//   return true;
// }

// type SubscribeParams = {
//   client: string;
//   stream: string;
//   params?: object;
// };

// async function main(): Promise<void> {
//   const systemName = 'system';
//   const backend = await connect();
//   const nats = new NatsClient({ backend, name: systemName });
//   const system = new System({ nats, systemNamespace: systemName });
//   const core = new Core({ nats });
  
//   const api = system.service('core');
  
//   api.method('subscribe', async (client, params) => {
//     console.log('subscribe', { client, params });
    
//     try {
//       if (!validate<SubscribeParams>(params)) {
//         throw new Error('invalid params');
//       }
      
//       const stream = await core.stream({ name: params.stream });
//       console.log({ stream });
      
//       const consumer = await core.consumer({ stream: stream.name, name: client });
//       console.log({ consumer });
      
//       return { stream, consumer } as SubscriptionInfo;
//     } catch (err) {
//       console.error(err);
//       throw err;
//     }
//   });
  
//   const cron = system.service('cron');
//   const tick = await cron.producer('tick');
//   const interval = setInterval(() => {
//     (async () => {
//       await tick.publish({ now: Date.now() });
//     })().catch(console.error);
//   }, parseDuration('1s', 'ms'));
  
//   const destroy = async () => {
//     await system.destroy();
//   };
  
//   process.once('SIGINT', () => {
//     destroy().catch(console.error);
//   });
// }

// main().catch(console.error);
