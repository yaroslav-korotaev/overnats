import { type Destructible, type SubscribeParams, type SubscribeResult } from './types';
import { hashOf, nanoid, distribute, sequence } from './utils';
import { type Backend } from './backend';
import { type ClientOptions, Client } from './client';
import { type Spawner } from './spawner';
import { type Bucket, type BucketCell, type BucketSlice } from './bucket';
import { type Watcher } from './watcher';
import { type Service } from './service';
import { type Timer } from './timer';

export type ProducerAcceptCallback<E> = (client: ProducerClient<E>) => Promise<Destructible>;

export type ProducerSpawnCallback<P, E> = (
  params: P,
  key: string,
) => Promise<ProducerAcceptCallback<E>>;

export type ProducerDistributionRecord = {
  shards: string[];
  replicas: number;
  distribution: Record<string, string[]>;
  revision: number;
  author: string;
};

export type ProducerInstanceRecord = {
  seen: string;
};

export type ProducerSubscriptionRecord<T> = {
  seen: string;
  stream: string;
  params: T;
};

export type ProducerOptions<P, E> = {
  backend: Backend;
  name: string;
  shards?: number;
  replicas?: number;
  callback: ProducerSpawnCallback<P, E>;
};

export class Producer<P, E> {
  private _backend: Backend;
  private _name: string;
  private _shards: string[];
  private _replicas: number;
  private _callback: ProducerSpawnCallback<P, E>;
  private _instance: string;
  private _crowd: Map<string, ProducerInstanceRecord>;
  private _spawner!: Spawner<void, { destroy: () => Promise<void> }>;
  private _bucket!: Bucket<unknown>;
  private _distribution!: BucketCell<ProducerDistributionRecord>;
  private _distributionWatch!: Watcher<ProducerDistributionRecord>;
  private _instances!: BucketSlice<ProducerInstanceRecord>;
  private _instancesWatch!: Watcher<ProducerInstanceRecord>;
  private _service!: Service;
  private _heartbeat!: Timer;
  
  constructor(options: ProducerOptions<P, E>) {
    const {
      backend,
      name,
      shards = 12,
      replicas = 2,
      callback,
    } = options;
    
    this._backend = backend;
    this._name = name;
    this._shards = sequence(shards);
    this._replicas = replicas;
    this._callback = callback;
    this._instance = nanoid();
    this._crowd = new Map();
  }
  
  private async _online(now: Date): Promise<void> {
    await this._instances.put(this._instance, {
      seen: now.toISOString(),
    });
  }
  
  private async _rebalance(revision: number): Promise<void> {
    await this._distribution.mutateUsing(async (value, use) => {
      if (value && value.revision >= revision) {
        return;
      }
      
      const shards = this._shards;
      const replicas = this._replicas;
      const instances = Array.from(this._crowd.keys());
      const distribution = distribute(instances, shards, replicas);
      
      await use({ shards, replicas, distribution, revision, author: this._instance });
    });
  }
  
  private async _pickup(shards: string[]): Promise<void> {
    const items = new Map<string, void>();
    
    for (const shard of shards) {
      items.set(shard, undefined);
    }
    
    await this._spawner.resetItems(items);
  }
  
  public async init(): Promise<void> {
    this._spawner = await this._backend.spawner(async (key, value) => {
      type SubscriptionRecord = ProducerSubscriptionRecord<P>;
      
      const subscriptions = this._bucket.slice<SubscriptionRecord>(`subscriptions.${key}`);
      
      const spawner = await this._backend.spawner<P, Destructible>(async (key, value) => {
        const accept = await this._callback(value, key);
        const workerClient = new ProducerClient({
          backend: this._backend,
          name: `producer.${this._name}.${key}`,
        });
        
        return await accept(workerClient);;
      });
      
      const watcher = await subscriptions.watch(async update => {
        if (update.operation == 'PUT') {
          await spawner.maybeRespawnItem(update.key, update.value.params);
        }
        
        if (update.operation == 'DEL') {
          await spawner.destroyItem(update.key);
        }
      }, { detach: true });
      
      return {
        async destroy() {
          await watcher.destroy();
          await spawner.destroy();
        },
      };
    });
    
    this._bucket = await this._backend.bucket(`producer.${this._name}`);
    this._distribution = this._bucket.cell('distribution');
    this._instances = this._bucket.slice('instances');
    
    this._distributionWatch = await this._distribution.watch(async update => {
      if (update.operation == 'PUT' && update.online) {
        const shards = update.value.distribution[this._instance];
        if (shards) {
          await this._pickup(shards);
        }
      }
    }, { detach: true });
    
    this._instancesWatch = await this._instances.watch(async update => {
      let disbalanced = false;
      
      if (update.operation == 'PUT') {
        if (!this._crowd.has(update.key)) {
          disbalanced = true;
        }
        
        this._crowd.set(update.key, update.value);
      }
      
      if (update.operation == 'DEL') {
        if (this._crowd.has(update.key)) {
          disbalanced = true;
        }
        
        this._crowd.delete(update.key);
      }
      
      if (update.online && disbalanced) {
        await this._rebalance(update.revision);
      }
    }, { detach: true });
    
    await this._online(new Date());
    
    this._service = await this._backend.service(`producer.${this._name}`);
    
    const subscriptions = this._bucket.slice('subscriptions');
    
    await this._service.method<SubscribeParams<P>, SubscribeResult>('subscribe', async ctx => {
      const now = new Date();
      const params = ctx.req.data.params;
      const hash = hashOf(params);
      const shard = this._shards[parseInt(hash.slice(-8), 16) % this._shards.length];
      const stream = `producer.${this._name}.${hash}`;
      
      await this._backend.jsStream(stream);
      
      await subscriptions.put(`${shard}.${hash}`, {
        seen: now.toISOString(),
        stream,
        params,
      });
      
      ctx.res.data = { stream };
    });
    
    const heartbeatInterval = this._backend.options.heartbeatInterval;
    const timerName = `producer.${this._name}.heartbeat`;
    this._heartbeat = await this._backend.localTimer(timerName, heartbeatInterval, async now => {
      await this._online(new Date(now));
    });
  }
  
  public async destroy(): Promise<void> {
    await this._heartbeat.destroy();
    await this._service.destroy();
    await this._instancesWatch.destroy();
    await this._distributionWatch.destroy();
    
    await this._instances.delete(this._instance);
    
    await this._bucket.destroy();
    await this._spawner.destroy();
  }
}

export type ProducerClientOptions = ClientOptions & {
};

export class ProducerClient<E> extends Client {
  constructor(options: ProducerClientOptions) {
    super(options);
  }
  
  public async emit(event: E): Promise<void> {
    await this._backend.js.publish(this._name, this._backend.encode(event));
  }
}
