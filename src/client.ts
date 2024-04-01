import { type AsyncCallback, type ReturnCallback, type Destructible } from './types';
import { type RetryOptions } from './utils';
import { Autodestructible } from './autodestructible';
import { type Backend } from './backend';
import { type SchedulerCallback, type Scheduler } from './scheduler';
import { type TimerCallback, type Timer } from './timer';
import { type SubscriptionCallback, type Subscription } from './subscription';
import { type Service } from './service';
import {
  type BucketBackendOptions,
  type Bucket,
  type BucketSlice,
  type BucketCell,
} from './bucket';
import { type SummonCallback, type SummonerMoreOptions, type Summoner } from './summoner';
import { type SpawnCallback, type Spawner } from './spawner';
import { type ProducerSpawnCallback, type Producer } from './producer';
import { type ConsumerCallback, type Consumer } from './consumer';

export type ClientSubscribeOptions = {
  broadcast?: boolean;
};

export type ClientOptions = {
  backend: Backend;
  name: string;
};

export class Client extends Autodestructible {
  protected _backend: Backend;
  protected _name: string;
  protected _bucket: Bucket<unknown> | undefined;
  
  public local: ClientLocal;
  
  constructor(options: ClientOptions) {
    super();
    
    const {
      backend,
      name,
    } = options;
    
    this._backend = backend;
    this._name = name;
    
    this.local = this.use(new ClientLocal({ backend, name }));
  }
  
  protected async _getDefaultBucket(): Promise<Bucket<unknown>> {
    if (!this._bucket) {
      this._bucket = this.use(await this.bucket('default'));
    }
    
    return this._bucket;
  }
  
  public async publish<T>(subject: string, data: T): Promise<void> {
    await this._backend.publish(`app.${subject}`, data);
  }
  
  public async subscribe<T>(
    subject: string,
    callback: SubscriptionCallback<T>,
    options?: ClientSubscribeOptions,
  ): Promise<Subscription<T>> {
    const prefix = `app.${this._name}`;
    const subscription = await this._backend.subscribe<T>(`${prefix}.${subject}`, async event => {
      await callback({ ...event, subject: event.subject.slice(prefix.length + 1) });
    }, {
      queue: (options?.broadcast) ? undefined : this._name,
    });
    
    return this.use(subscription);
  }
  
  public async call<P, R>(method: string, params: P): Promise<R> {
    return await this._backend.call(`app.${method}`, params);
  }
  
  public async service(name: string): Promise<Service> {
    const serviceName = `app.${this._name}.${name}`;
    const service = await this._backend.service(serviceName);
    
    return this.use(service);
  }
  
  public async bucket<T>(
    name: string,
    options?: Partial<BucketBackendOptions>,
  ): Promise<Bucket<T>> {
    const bucketName = `app.${this._name}.${name}`;
    const bucket = await this._backend.bucket<T>(bucketName, options);
    
    return this.use(bucket);
  }
  
  public async slice<T>(prefix: string): Promise<BucketSlice<T>> {
    const bucket = await this._getDefaultBucket();
    
    return bucket.slice(prefix);
  }
  
  public async cell<T>(key: string): Promise<BucketCell<T>> {
    const bucket = await this._getDefaultBucket();
    
    return bucket.cell(key);
  }
  
  public async lock(key: string, callback: AsyncCallback): Promise<void> {
    await this._backend.globalLock(`app.${this._name}.${key}`, callback);
  }
  
  public async spawner<T, I extends Destructible>(
    callback: SpawnCallback<T, I>,
  ): Promise<Spawner<T, I>> {
    const spawner = await this._backend.spawner(callback);
    
    return this.use(spawner);
  }
  
  public async summoner<P>(
    callback: SummonCallback<P>,
    options?: SummonerMoreOptions<P>,
  ): Promise<Summoner<P>> {
    const summoner = await this._backend.summoner<P>(callback, options);
    
    return this.use(summoner);
  }
  
  public async producer<P, E>(
    name: string,
    callback: ProducerSpawnCallback<P, E>,
  ): Promise<Producer<P, E>> {
    const producer = await this._backend.producer<P, E>(`${this._name}.${name}`, callback);
    
    return this.use(producer);
  }
  
  public async consumer<P, E>(
    name: string,
    producer: string,
    params: P,
    callback: ConsumerCallback<E>,
  ): Promise<Consumer<P, E>> {
    const consumer = await this._backend.consumer<P, E>(name, producer, params, callback);
    
    return this.use(consumer);
  }
}

export type ClientLocalOptions = {
  backend: Backend;
  name: string;
};

export class ClientLocal extends Autodestructible {
  private _backend: Backend;
  private _name: string;
  
  constructor(options: ClientLocalOptions) {
    super();
    
    const {
      backend,
      name,
    } = options;
    
    this._backend = backend;
    this._name = name;
  }
  
  public async retry<T>(
    callback: ReturnCallback<T>,
    options?: Partial<RetryOptions>,
  ): Promise<T> {
    return await this._backend.retry(callback, options);
  }
  
  public async scheduler(name: string, callback: SchedulerCallback): Promise<Scheduler> {
    const scheduler = await this._backend.localScheduler(`${this._name}.${name}`, callback);
    
    return this.use(scheduler);
  }
  
  public async timer(name: string, interval: number, callback: TimerCallback): Promise<Timer> {
    const timer = await this._backend.localTimer(name, interval, callback);
    
    return this.use(timer);
  }
}
