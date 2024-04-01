import {
  type Codec,
  type NatsConnection,
  type JetStreamClient,
  type JetStreamManager,
  type StreamInfo,
  type ConsumerInfo,
  RetentionPolicy,
  StorageType,
  DiscardPolicy,
  AckPolicy,
  DeliverPolicy,
  StringCodec,
} from 'nats';
import {
  type AsyncCallback,
  type UseCallback,
  type ReturnCallback,
  type ErrorCallback,
  type Destructible,
  type OvernatsGlobalOptions,
} from './types';
import { OvernatsError, deserializeError } from './errors';
import {
  type RetryOptions,
  retry,
  rootCause,
} from './utils';
import { Autoabortable, AutoabortableError } from './autoabortable';
import { type SchedulerCallback, Scheduler } from './scheduler';
import { type TimerCallback, Timer } from './timer';
import { type SummonCallback, type SummonerMoreOptions, Summoner } from './summoner';
import { Listener } from './listener';
import {
  type SubscriptionBackendOptions,
  type SubscriptionCallback,
  Subscription,
} from './subscription';
import { Service } from './service';
import { type BucketBackendOptions, Bucket } from './bucket';
import { type SpawnCallback, Spawner } from './spawner';
import { type ProducerSpawnCallback, Producer } from './producer';
import { type ConsumerCallback, Consumer } from './consumer';

export type BackendOptions = {
  core: NatsConnection;
  options?: Partial<OvernatsGlobalOptions>;
  uncaughtException: ErrorCallback;
};

export class Backend {
  public autoabortable: Autoabortable;
  public core: NatsConnection;
  public js!: JetStreamClient;
  public jsm!: JetStreamManager;
  public locks!: Bucket<void>;
  public codec: Codec<string>;
  public options: OvernatsGlobalOptions;
  public uncaughtException: ErrorCallback;
  
  constructor(options: BackendOptions) {
    const {
      core,
      options: globalOptions,
      uncaughtException,
    } = options;
    
    this.autoabortable = new Autoabortable();
    this.core = core;
    this.codec = StringCodec();
    this.options = Object.assign({
      heartbeatInterval: 3000, //30_000,
      lockTimeout: 10_000,
    }, globalOptions);
    this.uncaughtException = err => {
      if (rootCause(err) instanceof AutoabortableError) {
        return;
      }
      
      uncaughtException(err);
    };
  }
  
  public async init(): Promise<void> {
    this.js = this.core.jetstream();
    this.jsm = await this.core.jetstreamManager();
    this.locks = await this.bucket('locks', {
      replicas: 1,
      ttl: this.options.lockTimeout,
      storage: StorageType.Memory,
    });
  }
  
  public async destroy(): Promise<void> {
    this.autoabortable.abort();
    await this.core.drain();
  }
  
  public encode(data: unknown): Uint8Array {
    return this.codec.encode(JSON.stringify(data));
  }
  
  public decode(data: Uint8Array): unknown {
    const value = (data.length > 0) ? JSON.parse(this.codec.decode(data)) : undefined;
    
    return value;
  }
  
  public async jsStream(name: string): Promise<StreamInfo> {
    const streamInfo = await this.jsm.streams.add({
      name: name.replaceAll('.', '_'),
      retention: RetentionPolicy.Interest,
      storage: StorageType.File,
      subjects: [name],
      max_msgs: 100_000,
      max_age: 2 * 60 * 60 * 1000 * 1000 * 1000, // 2 hours in nanoseconds
      max_bytes: 100 * 1024 * 1024, // 100 MB
      max_msg_size: 100 * 1024, // 100 KB
      discard: DiscardPolicy.Old,
      // num_replicas: 3,
    });
    
    // console.log({ streamInfo });
    
    return streamInfo;
  }
  
  public async jsConsumer(stream: string, name: string): Promise<ConsumerInfo> {
    const consumerInfo = await this.jsm.consumers.add(stream.replaceAll('.', '_'), {
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.New,
      durable_name: name.replaceAll('.', '_'),
    });
    
    // console.log({ consumerInfo });
    
    return consumerInfo;
  }
  
  public async listen<T>(
    name: string,
    iterable: AsyncIterable<T>,
    callback: UseCallback<T>,
  ): Promise<Listener<T>> {
    const listener = new Listener({
      trapdoor: this,
      name,
      iterable,
      callback,
    });
    
    await listener.init();
    
    return listener;
  }
  
  public async publish<T>(subject: string, data: T): Promise<void> {
    this.core.publish(subject, this.encode(data));
  }
  
  public async subscribe<T>(
    subject: string,
    callback: SubscriptionCallback<T>,
    options?: SubscriptionBackendOptions,
  ): Promise<Subscription<T>> {
    const subscription = new Subscription({
      backend: this,
      subject,
      options,
      callback,
    });
    
    await subscription.init();
    
    return subscription;
  }
  
  public async call<P, R>(method: string, params: P): Promise<R> {
    try {
      const message = await this.core.request(method, this.encode(params));
      const response = this.decode(message.data);
      
      if (response && typeof response == 'object') {
        if ('result' in response) {
          return response.result as R;
        }
        
        if ('error' in response) {
          throw deserializeError(response.error);
        }
        
        return undefined as R;
      }
      
      throw new Error('invalid response');
    } catch (err) {
      throw new OvernatsError('request error', {
        cause: err,
        details: { method },
      });
    }
  }
  
  public async service(name: string): Promise<Service> {
    const service = new Service({
      backend: this,
      name,
    });
    
    return service;
  }
  
  public async bucket<T>(
    name: string,
    options?: Partial<BucketBackendOptions>,
  ): Promise<Bucket<T>> {
    const bucket = new Bucket<T>({
      backend: this,
      name,
      options,
    });
    
    await bucket.init();
    
    return bucket;
  }
  
  public async spawner<T, I extends Destructible>(
    callback: SpawnCallback<T, I>,
  ): Promise<Spawner<T, I>> {
    const spawner = new Spawner({
      callback,
    });
    
    return spawner;
  }
  
  public async summoner<P>(
    callback: SummonCallback<P>,
    options?: SummonerMoreOptions<P>,
  ): Promise<Summoner<P>> {
    const summoner = new Summoner({
      compare: options?.compare,
      callback,
    });
    
    return summoner;
  }
  
  public async producer<P, E>(
    name: string,
    callback: ProducerSpawnCallback<P, E>,
  ): Promise<Producer<P, E>> {
    const producer = new Producer<P, E>({
      backend: this,
      name,
      callback,
    });
    
    await producer.init();
    
    return producer;
  }
  
  public async consumer<P, E>(
    name: string,
    producer: string,
    params: P,
    callback: ConsumerCallback<E>,
  ): Promise<Consumer<P, E>> {
    const consumer = new Consumer<P, E>({
      backend: this,
      name,
      producer,
      params,
      callback,
    });
    
    await consumer.init();
    
    return consumer;
  }
  
  public async globalLock(key: string, callback: AsyncCallback): Promise<void> {
    await this.locks.lock(key, callback);
  }
  
  public async retry<T>(
    callback: ReturnCallback<T>,
    options?: Partial<RetryOptions>,
  ): Promise<T> {
    if (options?.signal) {
      return await retry(callback, options);
    }
    
    return await this.autoabortable.execute(async signal => {
      return await retry(callback, { ...options, signal });
    });
  }
  
  public async localTimer(
    name: string,
    interval: number,
    callback: TimerCallback,
  ): Promise<Timer> {
    const timer = new Timer({
      backend: this,
      interval,
      callback: async now => {
        try {
          await callback(now);
        } catch (err) {
          this.uncaughtException(new OvernatsError('timer callback failed', {
            cause: err,
            details: { timer: name },
          }));
        }
      },
    });
    
    await timer.init();
    
    return timer;
  }
  
  public async localScheduler(name: string, callback: SchedulerCallback): Promise<Scheduler> {
    const scheduler = new Scheduler({
      callback: async (now, schedule) => {
        try {
          await callback(now, schedule);
        } catch (err) {
          this.uncaughtException(new OvernatsError('scheduler callback failed', {
            cause: err,
            details: { scheduler: name },
          }));
        }
      },
    });
    
    return scheduler;
  }
}
