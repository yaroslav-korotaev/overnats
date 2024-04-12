import { type KV, StorageType } from 'nats';
import { Autodestructible } from './autodestructible';
import { type Core } from './core';
import { type WatcherCallback, Watcher } from './watcher';

export type BucketMutateCallback<T, R> = (
  value: T | undefined,
  write: (next: T) => Promise<T>,
) => Promise<R>;

export type BucketWatchOptions = {
  filter?: string;
  detach?: boolean;
};

export type BucketBackendOptions = {
  replicas: number;
  maxBytes: number;
  maxValueSize: number;
  ttl: number;
  storage: 'file' | 'memory';
};

export type BucketOptions = {
  core: Core;
  name: string;
  options?: Partial<BucketBackendOptions>;
};

export class Bucket<T> extends Autodestructible {
  private _core: Core;
  private _name: string;
  private _options: Partial<BucketBackendOptions>;
  private _kv!: KV;
  
  constructor(options: BucketOptions) {
    super();
    
    const {
      core,
      name,
      options: backendOptions = {},
    } = options;
    
    this._core = core;
    this._name = name;
    this._options = backendOptions;
  }
  
  public async init(): Promise<void> {
    this._kv = await this._core.js.views.kv(this._name.replaceAll('.', '_'), {
      replicas: this._options.replicas,
      max_bytes: this._options.maxBytes,
      maxValueSize: this._options.maxValueSize,
      ttl: this._options.ttl,
      storage: this._options.storage as StorageType,
    });
  }
  
  public override async destroy(): Promise<void> {
    await super.destroy();
  }
  
  public slice<T>(prefix: string): BucketSlice<T> {
    return new BucketSlice<T>({
      parent: this as Bucket<unknown>,
      prefix,
    });
  }
  
  public cell<T>(key: string): BucketCell<T> {
    return new BucketCell<T>({
      parent: this as Bucket<unknown>,
      key,
    });
  }
  
  public async get(key: string): Promise<T | undefined> {
    const entry = await this._kv.get(key);
    if (!entry || entry.operation != 'PUT') {
      return undefined;
    }
    
    return this._core.decode(entry.value) as T;
  }
  
  public async put(key: string, value: T): Promise<void> {
    await this._kv.put(key, this._core.encode(value));
  }
  
  public async delete(key: string): Promise<void> {
    await this._kv.delete(key);
  }
  
  public async mutate<R>(
    key: string,
    callback: BucketMutateCallback<T, R>,
  ): Promise<R> {
    const entry = await this._kv.get(key);
    
    if (entry) {
      const prev = (entry.operation == 'PUT' && entry.value.length > 0)
        ? this._core.decode(entry.value) as T
        : undefined
      ;
      
      return await callback(prev, async next => {
        await this._kv.update(key, this._core.encode(next), entry.revision);
        
        return next;
      });
    } else {
      return await callback(undefined, async next => {
        await this._kv.create(key, this._core.encode(next));
        
        return next;
      });
    }
  }
  
  public async keys(filter?: string): Promise<string[]> {
    const iterator = await this._kv.keys(filter);
    const keys = [];
    
    for await (const key of iterator) {
      keys.push(key);
    }
    
    return keys;
  }
  
  public async watch(
    callback: WatcherCallback<T>,
    options?: BucketWatchOptions,
  ): Promise<Watcher<T>> {
    const {
      filter,
      detach = false,
    } = options ?? {};
    
    const watcher = new Watcher<T>({
      core: this._core,
      kv: this._kv,
      filter,
      callback,
    });
    
    await watcher.init();
    
    if (detach) {
      return watcher;
    }
    
    return this.use(watcher);
  }
}

export type BucketSliceOptions = {
  parent: Bucket<unknown>;
  prefix: string;
};

export class BucketSlice<T> {
  private _parent: Bucket<T>;
  private _prefix: string;
  
  constructor(options: BucketSliceOptions) {
    const {
      parent,
      prefix,
    } = options;
    
    this._parent = parent as Bucket<T>;
    this._prefix = prefix;
  }
  
  public slice<N = T>(prefix: string): BucketSlice<N> {
    return new BucketSlice<N>({
      parent: this._parent as Bucket<unknown>,
      prefix: `${this._prefix}.${prefix}`,
    });
  }
  
  public cell<N = T>(key: string): BucketCell<N> {
    return new BucketCell<N>({
      parent: this._parent as Bucket<unknown>,
      key: `${this._prefix}.${key}`,
    });
  }
  
  public async get(key: string): Promise<T | undefined> {
    return await this._parent.get(`${this._prefix}.${key}`);
  }
  
  public async put(key: string, value: T): Promise<void> {
    await this._parent.put(`${this._prefix}.${key}`, value);
  }
  
  public async delete(key: string): Promise<void> {
    await this._parent.delete(`${this._prefix}.${key}`);
  }
  
  public async mutate<R>(
    key: string,
    callback: BucketMutateCallback<T, R>,
  ): Promise<R> {
    return await this._parent.mutate(`${this._prefix}.${key}`, callback);
  }
  
  public async keys(filter?: string): Promise<string[]> {
    return await this._parent.keys(`${this._prefix}.${filter ?? '>'}`);
  }
  
  public async watch(
    callback: WatcherCallback<T>,
    options?: BucketWatchOptions,
  ): Promise<Watcher<T>> {
    const filter = `${this._prefix}.${options?.filter ?? '>'}`;
    
    return await this._parent.watch(async update => {
      await callback({
        ...update,
        key: update.key.slice(this._prefix.length + 1),
      });
    }, { ...options, filter });
  }
}

export type BucketCellOptions = {
  parent: Bucket<unknown>;
  key: string;
};

export class BucketCell<T> {
  private _parent: Bucket<T>;
  private _key: string;
  
  constructor(options: BucketCellOptions) {
    const {
      parent,
      key,
    } = options;
    
    this._parent = parent as Bucket<T>;
    this._key = key;
  }
  
  public async get(): Promise<T | undefined> {
    return await this._parent.get(this._key);
  }
  
  public async put(value: T): Promise<void> {
    await this._parent.put(this._key, value);
  }
  
  public async delete(): Promise<void> {
    await this._parent.delete(this._key);
  }
  
  public async mutate<R>(callback: BucketMutateCallback<T, R>): Promise<R> {
    return await this._parent.mutate(this._key, callback);
  }
  
  public async watch(
    callback: WatcherCallback<T>,
    options?: BucketWatchOptions,
  ): Promise<Watcher<T>> {
    return await this._parent.watch(callback, { ...options, filter: this._key });
  }
}
