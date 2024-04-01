import { type KV, StorageType } from 'nats';
import { type AsyncCallback, type MutateCallback, type MutateUsingCallback } from './types';
import { OvernatsError } from './errors';
import { isWrongLastSequenceError, tryWithFallback, anyway, retry } from './utils';
import { Autodestructible } from './autodestructible';
import { type Backend } from './backend';
import { type WatcherCallback, Watcher } from './watcher';

export type BucketMutateOptions = {
  revision?: number;
};

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
  backend: Backend;
  name: string;
  options?: Partial<BucketBackendOptions>;
};

export class Bucket<T> extends Autodestructible {
  private _backend: Backend;
  private _name: string;
  private _options: Partial<BucketBackendOptions>;
  private _kv!: KV;
  
  constructor(options: BucketOptions) {
    super();
    
    const {
      backend,
      name,
      options: backendOptions = {},
    } = options;
    
    this._backend = backend;
    this._name = name;
    this._options = backendOptions;
  }
  
  public async init(): Promise<void> {
    this._kv = await this._backend.js.views.kv(this._name.replaceAll('.', '_'), {
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
    
    return this._backend.decode(entry.value) as T;
  }
  
  public async put(key: string, value: T): Promise<void> {
    await this._kv.put(key, this._backend.encode(value));
  }
  
  public async delete(key: string): Promise<void> {
    await this._kv.delete(key);
  }
  
  public async mutate(
    key: string,
    callback: MutateCallback<T>,
  ): Promise<T> {
    const entry = await this._kv.get(key);
    
    if (entry) {
      const prev = (entry.value.length > 0) ? this._backend.decode(entry.value) as T : undefined;
      const next = await callback(prev);
      
      await this._kv.update(key, this._backend.encode(next), entry.revision);
      
      return next;
    } else {
      const next = await callback(undefined);
      
      await this._kv.create(key, this._backend.encode(next));
      
      return next;
    }
  }
  
  public async mutateUsing(
    key: string,
    callback: MutateUsingCallback<T>,
  ): Promise<void> {
    await retry(async () => {
      const entry = await this._kv.get(key);
      
      if (entry) {
        const prev = (entry.value.length > 0) ? this._backend.decode(entry.value) as T : undefined;
        
        await callback(prev, async next => {
          await this._kv.update(key, this._backend.encode(next), entry.revision);
        });
      } else {
        await callback(undefined, async next => {
          await this._kv.create(key, this._backend.encode(next));
        });
      }
    }, {
      when: isWrongLastSequenceError,
    });
  }
  
  // public async maybeMutate(
  //   key: string,
  //   callback: MutateCallback<T>,
  //   options?: BucketMutateOptions,
  // ): Promise<void> {
  //   try {
  //     await this.mutate(key, callback, options);
  //   } catch (err) {
  //     if  {
  //       return;
  //     }
      
  //     throw err;
  //   }
  // }
  
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
      backend: this._backend,
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
  
  public async lock(key: string, callback: AsyncCallback): Promise<void> {
    try {
      await retry(async () => {
        const revision = await tryWithFallback(async () => {
          return await this._kv.create(key, this._backend.encode(undefined));
        }, async err => {
          if (isWrongLastSequenceError(err)) {
            throw new Error('oops');
          }
          
          throw err;
        });
        
        await anyway(async () => {
          await callback();
        }, async () => {
          try {
            await this._kv.delete(key, { previousSeq: revision });
          } catch (err) {
            if (isWrongLastSequenceError(err)) {
              return;
            }
            
            throw err;
          }
        });
      }, {
        when: err => err instanceof Error && err.message == 'oops',
        // retries: 20,
        retries: 0,
        maxDelay: 2_000,
      });
    } catch (err) {
      if (err instanceof Error && err.message == 'oops') {
        throw new OvernatsError('lock failed', { details: { key } });
      }
      
      throw err;
    }
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
  
  public async get(key: string): Promise<T | undefined> {
    return await this._parent.get(`${this._prefix}.${key}`);
  }
  
  public async put(key: string, value: T): Promise<void> {
    await this._parent.put(`${this._prefix}.${key}`, value);
  }
  
  public async delete(key: string): Promise<void> {
    await this._parent.delete(`${this._prefix}.${key}`);
  }
  
  public async mutate(key: string, callback: MutateCallback<T>): Promise<T> {
    return await this._parent.mutate(`${this._prefix}.${key}`, callback);
  }
  
  public async mutateUsing(key: string, callback: MutateUsingCallback<T>): Promise<void> {
    await this._parent.mutateUsing(`${this._prefix}.${key}`, callback);
  }
  
  // public async maybeMutate(
  //   key: string,
  //   callback: MutateCallback<T>,
  //   options?: BucketMutateOptions,
  // ): Promise<void> {
  //   await this._parent.maybeMutate(`${this._prefix}.${key}`, callback, options);
  // }
  
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
  
  public async mutate(callback: MutateCallback<T>): Promise<T> {
    return await this._parent.mutate(this._key, callback);
  }
  
  public async mutateUsing(callback: MutateUsingCallback<T>): Promise<void> {
    await this._parent.mutateUsing(this._key, callback);
  }
  
  // public async maybeMutate(
  //   callback: MutateCallback<T>,
  //   options?: BucketMutateOptions,
  // ): Promise<void> {
  //   await this._parent.maybeMutate(this._key, callback, options);
  // }
  
  public async watch(
    callback: WatcherCallback<T>,
    options?: BucketWatchOptions,
  ): Promise<Watcher<T>> {
    return await this._parent.watch(callback, { ...options, filter: this._key });
  }
}
