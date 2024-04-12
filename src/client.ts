import { type Telemetry } from 'universe-types';
import {
  type Callback,
  type Destructible,
  type SpawnCallback,
  type RetryOptions,
  Container,
} from 'ominous';
import { type Core } from './core';
import { type SchedulerCallback, type Scheduler } from './scheduler';
import { type TimerCallback, type Timer } from './timer';
import { type MutexCell } from './multimutex';
import { type SubscriptionCallback, type Subscription } from './subscription';
import { type Service } from './service';
import {
  type BucketBackendOptions,
  type Bucket,
  type BucketSlice,
  type BucketCell,
} from './bucket';
import { type SummonCallback, type SummonerMoreOptions, type Summoner } from './summoner';
import { type SpawnerCallback, type Spawner } from './spawner';

export type ClientStackParams = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  container: Container;
};

export class ClientStack {
  public telemetry: Telemetry;
  public core: Core;
  public name: string;
  public container: Container;
  
  constructor(params: ClientStackParams) {
    const {
      telemetry,
      core,
      name,
      container,
    } = params;
    
    this.telemetry = telemetry;
    this.core = core;
    this.name = name;
    this.container = container;
  }
  
  protected async _use<T extends Destructible>(spawn: SpawnCallback<T>): Promise<T> {
    return this.container.use(spawn);
  }
  
  public async init(): Promise<void> {
    // no-op
  }
  
  public async destroy(): Promise<void> {
    await this.container.destroy();
    this.telemetry.destroy();
  }
}

export type ClientSubscribeOptions = {
  broadcast?: boolean;
};

export type ClientParams = ClientStackParams;

export class Client extends ClientStack {
  private _bucket: Bucket<unknown> | undefined;
  
  public local: ClientLocal;
  
  constructor(params: ClientParams) {
    super(params);
    
    this._bucket = undefined;
    
    this.local = this._local();
  }
  
  protected _local(): ClientLocal {
    return new ClientLocal({
      telemetry: this.telemetry,
      core: this.core,
      name: this.name,
      container: this.container,
    });
  }
  
  protected async _getDefaultBucket(): Promise<Bucket<unknown>> {
    if (!this._bucket) {
      this._bucket = await this._use(async () => {
        return await this.core.bucket('default');
      });
    }
    
    return this._bucket;
  }
  
  public mutex(key: string): MutexCell {
    return this.core.mutex(`app.${this.name}.${key}`);
  }
  
  public async publish<T>(subject: string, data: T): Promise<void> {
    await this.core.publish(`app.${subject}`, data);
  }
  
  public async subscribe<T>(
    subject: string,
    callback: SubscriptionCallback<T>,
    options?: ClientSubscribeOptions,
  ): Promise<Subscription<T>> {
    return await this._use(async () => {
      const prefix = `app.${this.name}`;
      const subscription = await this.core.subscribe<T>(`${prefix}.${subject}`, async event => {
        await callback({ ...event, subject: event.subject.slice(prefix.length + 1) });
      }, {
        queue: (options?.broadcast) ? undefined : this.name,
      });
      
      return subscription;
    });
  }
  
  public async call<P, R>(method: string, params: P): Promise<R> {
    return await this.core.call(`app.${method}`, params);
  }
  
  public async service(name: string): Promise<Service> {
    return await this._use(async () => {
      const serviceName = `app.${this.name}.${name}`;
      const service = await this.core.service(serviceName);
      
      return service;
    });
  }
  
  public async bucket<T>(
    name: string,
    options?: Partial<BucketBackendOptions>,
  ): Promise<Bucket<T>> {
    return await this._use(async () => {
      const bucketName = `app.${this.name}.${name}`;
      const bucket = await this.core.bucket<T>(bucketName, options);
      
      return bucket;
    });
  }
  
  public async slice<T>(prefix: string): Promise<BucketSlice<T>> {
    const bucket = await this._getDefaultBucket();
    
    return bucket.slice(prefix);
  }
  
  public async cell<T>(key: string): Promise<BucketCell<T>> {
    const bucket = await this._getDefaultBucket();
    
    return bucket.cell(key);
  }
  
  public async spawner<T, I extends Destructible>(
    callback: SpawnerCallback<T, I>,
  ): Promise<Spawner<T, I>> {
    return await this._use(async () => {
      const spawner = await this.core.spawner<T, I>(callback);
      
      return spawner;
    });
  }
  
  public async summoner<P>(
    callback: SummonCallback<P>,
    options?: SummonerMoreOptions<P>,
  ): Promise<Summoner<P>> {
    return await this._use(async () => {
      const summoner = await this.core.summoner<P>(callback, options);
      
      return summoner;
    });
  }
}

export type ClientLocalParams = ClientStackParams;

export class ClientLocal extends ClientStack {
  constructor(params: ClientLocalParams) {
    super(params);
  }
  
  public async retry<T>(
    callback: Callback<T>,
    options?: Partial<RetryOptions>,
  ): Promise<T> {
    return await this.core.retry(callback, options);
  }
  
  public async scheduler(name: string, callback: SchedulerCallback): Promise<Scheduler> {
    return await this._use(async () => {
      const scheduler = await this.core.localScheduler(`${this.name}.${name}`, callback);
      
      return scheduler;
    });
  }
  
  public async timer(name: string, interval: number, callback: TimerCallback): Promise<Timer> {
    return await this._use(async () => {
      const timer = await this.core.localTimer(name, interval, callback);
      
      return timer;
    });
  }
}
