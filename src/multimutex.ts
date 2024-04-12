import { type Callback, defaults, fallback, anyway, retry } from 'ominous';
import { type KV, StorageType } from 'nats';
import { OvernatsError } from './errors';
import { isWrongLastSequenceError } from './utils';
import { type Core } from './core';

export type MutexBucketOptions = {
  bucketName: string;
  ttl: number;
};

export type MutexBucketParams = {
  core: Core;
  options?: Partial<MutexBucketOptions>;
};

export class MutexBucket {
  private _core: Core;
  private _options: MutexBucketOptions;
  private _kv!: KV;
  
  constructor(params: MutexBucketParams) {
    const {
      core,
      options,
    } = params;
    
    this._core = core;
    this._options = defaults(options, {
      bucketName: 'lock',
      ttl: this._core.options.lockTimeout,
    });
  }
  
  public async init(): Promise<void> {
    this._kv = await this._core.js.views.kv(this._options.bucketName, {
      replicas: 1,
      ttl: this._options.ttl,
      storage: StorageType.Memory,
    });
  }
  
  public async destroy(): Promise<void> {
    // no-op
  }
  
  public mutex(key: string): MutexCell {
    return new MutexCell({
      core: this._core,
      kv: this._kv,
      key,
    });
  }
}

export type MutexCellParams = {
  core: Core;
  kv: KV;
  key: string;
};

export class MutexCell {
  private _core: Core;
  private _kv: KV;
  private _key: string;
  
  constructor(params: MutexCellParams) {
    const {
      core,
      kv,
      key,
    } = params;
    
    this._core = core;
    this._kv = kv;
    this._key = key;
  }
  
  public async lock(callback: Callback): Promise<void> {
    try {
      await retry(async () => {
        const revision = await fallback(async () => {
          return await this._kv.create(this._key, this._core.encode(undefined));
        }, async err => {
          if (isWrongLastSequenceError(err)) {
            throw new OvernatsMutexLockedError();
          }
          
          throw err;
        });
        
        await anyway(async () => {
          await callback();
        }, async () => {
          try {
            await this._kv.delete(this._key, { previousSeq: revision });
          } catch (err) {
            if (isWrongLastSequenceError(err)) {
              return;
            }
            
            throw err;
          }
        });
      }, {
        when: err => err instanceof OvernatsMutexLockedError,
        maxDelay: 2_000,
      });
    } catch (err) {
      if (err instanceof OvernatsMutexLockedError) {
        throw new OvernatsError('cannot acquire lock', { details: { key: this._key } });
      }
      
      throw err;
    }
  }
}

export class OvernatsMutexLockedError extends OvernatsError {
  constructor() {
    super('mutex locked');
  }
}
