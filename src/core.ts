import { type Telemetry } from 'universe-types';
import {
  type Callback,
  type UseCallback,
  type ErrorCallback,
  type Destructible,
  type RetryOptions,
  defaults,
  retry,
} from 'ominous';
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
  type OvernatsGlobalOptions,
} from './types';
import { OvernatsError, deserializeError } from './errors';
import { rootCause } from './utils';
import { Autoabortable, AutoabortableError } from './autoabortable';
import { type SchedulerCallback, Scheduler } from './scheduler';
import { type TimerCallback, Timer } from './timer';
import { type SummonCallback, type SummonerMoreOptions, Summoner } from './summoner';
import { type MutexCell, MutexBucket } from './multimutex';
import { Listener } from './listener';
import {
  type SubscriptionBackendOptions,
  type SubscriptionCallback,
  Subscription,
} from './subscription';
import { Service } from './service';
import { type BucketBackendOptions, Bucket } from './bucket';
import { type SpawnerCallback, Spawner } from './spawner';
import { type SliceSpawnerFactory, SliceSpawner } from './slice-spawner';
import { type StreamFactory, Stream, StreamSubscription, StreamPipe } from './stream';

export type CoreOptions = {
  telemetry: Telemetry;
  nats: NatsConnection;
  options?: Partial<OvernatsGlobalOptions>;
  uncaughtException: ErrorCallback;
};

export class Core {
  public telemetry: Telemetry;
  public autoabortable: Autoabortable;
  public nats: NatsConnection;
  public js!: JetStreamClient;
  public jsm!: JetStreamManager;
  public codec: Codec<string>;
  public options: OvernatsGlobalOptions;
  public replicas: number;
  public uncaughtException: ErrorCallback;
  public multimutex!: MutexBucket;
  public registry!: Bucket<unknown>;
  
  public stream: StreamFactory;
  public sliceSpawner: SliceSpawnerFactory;
  
  constructor(options: CoreOptions) {
    const {
      telemetry,
      nats,
      options: globalOptions,
      uncaughtException,
    } = options;
    
    this.telemetry = telemetry;
    this.autoabortable = new Autoabortable();
    this.nats = nats;
    this.codec = StringCodec();
    this.options = defaults(globalOptions, {
      heartbeatInterval: 30_000,
      lockTimeout: 10_000,
    });
    this.replicas = 1;
    this.uncaughtException = err => {
      if (rootCause(err) instanceof AutoabortableError) {
        return;
      }
      
      uncaughtException(err);
    };
    
    const streamFactory: StreamFactory = async (telemetry, name, options) => {
      const stream = new Stream({ telemetry, core: this, name, options });
      await stream.init();
      
      return stream;
    };
    streamFactory.subscribe = async (telemetry, name, ref, callback, options) => {
      const subscription = new StreamSubscription({
        telemetry,
        core: this,
        name,
        ref,
        callback,
        options,
      });
      await subscription.init();
      
      return subscription;
    };
    streamFactory.pipe = async (telemetry, name, callback, options) => {
      const pipe = new StreamPipe({ telemetry, core: this, name, callback, options });
      await pipe.init();
      
      return pipe;
    };
    this.stream = streamFactory;
    
    this.sliceSpawner = async (name, slice, callback) => {
      const spawner = new SliceSpawner({ core: this, name, slice, callback });
      await spawner.init();
      
      return spawner;
    };
  }
  
  public async init(): Promise<void> {
    this.js = this.nats.jetstream();
    this.jsm = await this.nats.jetstreamManager();
    this.multimutex = new MutexBucket({ core: this });
    await this.multimutex.init();
    this.registry = await this.bucket('registry');
  }
  
  public async destroy(): Promise<void> {
    this.autoabortable.abort();
    await this.registry.destroy();
    await this.nats.drain();
    this.telemetry.destroy();
  }
  
  public encode(data: unknown): Uint8Array {
    return this.codec.encode(JSON.stringify(data));
  }
  
  public decode(data: Uint8Array): unknown {
    const value = (data.length > 0) ? JSON.parse(this.codec.decode(data)) : undefined;
    
    return value;
  }
  
  public warn(err: unknown): void {
    console.warn('core warning', { err });
  }
  
  public error(err: unknown): void {
    console.error('core error', { err });
  }
  
  public async nothrow(callback: Callback, details?: Record<string, unknown>): Promise<void> {
    try {
      await callback();
    } catch (err) {
      this.error(new OvernatsError('uncaught exception', { cause: err, details }));
    }
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
    
    return streamInfo;
  }
  
  public async jsConsumer(stream: string, name: string): Promise<ConsumerInfo> {
    const consumerInfo = await this.jsm.consumers.add(stream.replaceAll('.', '_'), {
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.New,
      durable_name: name.replaceAll('.', '_'),
    });
    
    return consumerInfo;
  }
  
  public mutex(key: string): MutexCell {
    return this.multimutex.mutex(key);
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
    this.nats.publish(subject, this.encode(data));
  }
  
  public async subscribe<T>(
    subject: string,
    callback: SubscriptionCallback<T>,
    options?: SubscriptionBackendOptions,
  ): Promise<Subscription<T>> {
    const subscription = new Subscription({
      core: this,
      subject,
      options,
      callback,
    });
    
    await subscription.init();
    
    return subscription;
  }
  
  public async call<P, R>(method: string, params: P): Promise<R> {
    this.telemetry.trace({ method }, 'call');
    
    try {
      const message = await this.nats.request(method, this.encode(params), {
        timeout: 5000,
      });
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
      core: this,
      name,
    });
    
    return service;
  }
  
  public async bucket<T>(
    name: string,
    options?: Partial<BucketBackendOptions>,
  ): Promise<Bucket<T>> {
    const bucket = new Bucket<T>({
      core: this,
      name,
      options,
    });
    
    await bucket.init();
    
    return bucket;
  }
  
  public async spawner<T, I extends Destructible>(
    callback: SpawnerCallback<T, I>,
  ): Promise<Spawner<T, I>> {
    const spawner = new Spawner<T, I>({
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
  
  public async retry<T>(
    callback: Callback<T>,
    options?: Partial<RetryOptions>,
  ): Promise<T> {
    const wrapper = async () => {
      return await this.telemetry.span('retry', async span => {
        try {
          return await callback();
        } catch (err) {
          span.trace({ err });
          throw err;
        }
      });
    };
    
    if (options?.signal) {
      return await retry(wrapper, options);
    }
    
    return await this.autoabortable.execute(async signal => {
      return await retry(wrapper, { ...options, signal });
    });
  }
  
  public async localTimer(
    name: string,
    interval: number,
    callback: TimerCallback,
  ): Promise<Timer> {
    const timer = new Timer({
      core: this,
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
