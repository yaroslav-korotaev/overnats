import { type Destructible } from './types';
import { OvernatsError } from './errors';
import { Mutex, hashOf } from './utils';

export type SpawnCallback<T, I extends Destructible> = (key: string, value: T) => Promise<I>;

export type ForEachCallback<I> = (item: I) => Promise<void>;

export type SpawnerOptions<T, I extends Destructible> = {
  callback: SpawnCallback<T, I>;
};

export class Spawner<T, I extends Destructible> {
  private _callback: SpawnCallback<T, I>;
  private _mutex: Mutex;
  private _items: Map<string, { hash: string, item: I }>;
  
  constructor(options: SpawnerOptions<T, I>) {
    const {
      callback,
    } = options;
    
    this._callback = callback;
    this._mutex = new Mutex();
    this._items = new Map();
  }
  
  private async _spawnItem(key: string, value: T, hash: string): Promise<void> {
    try {
      const item = await this._callback(key, value);
      this._items.set(key, { hash, item });
    } catch (err) {
      throw new OvernatsError('spawner spawn item error', {
        cause: err,
        details: { key },
      });
    }
  }
  
  private async _destroyItem(key: string): Promise<void> {
    try {
      const entry = this._items.get(key);
      if (!entry) {
        return;
      }
      
      await entry.item.destroy();
      this._items.delete(key);
    } catch (err) {
      throw new OvernatsError('spawner destroy item error', {
        cause: err,
        details: { key },
      });
    }
  }
  
  private async _maybeRespawnItem(key: string, value: T): Promise<void> {
    const hash = hashOf(value);
    const entry = this._items.get(key);
    
    if (entry) {
      if (hash == entry.hash) {
        return;
      }
      
      await this._destroyItem(key);
    }
    
    await this._spawnItem(key, value, hash);
  }
  
  public async spawnItem(key: string, value: T): Promise<void> {
    await this._mutex.lock(async () => {
      if (this._items.has(key)) {
        throw new OvernatsError('spawner item already exists', {
          details: { key },
        });
      }
      
      await this._spawnItem(key, value, hashOf(value));
    });
  }
  
  public async destroyItem(key: string): Promise<void> {
    await this._mutex.lock(async () => {
      await this._destroyItem(key);
    });
  }
  
  public async maybeRespawnItem(key: string, value: T): Promise<void> {
    await this._mutex.lock(async () => {
      await this._maybeRespawnItem(key, value);
    });
  }
  
  public async resetItems(items: Map<string, T>): Promise<void> {
    await this._mutex.lock(async () => {
      const left = new Set(this._items.keys());
      
      for (const [key, value] of items.entries()) {
        await this._maybeRespawnItem(key, value);
        left.delete(key);
      }
      
      for (const key of left) {
        await this._destroyItem(key);
      }
    });
  }
  
  public async forEach(callback: ForEachCallback<I>): Promise<void> {
    await this._mutex.lock(async () => {
      for (const entry of this._items.values()) {
        await callback(entry.item);
      }
    });
  }
  
  public async destroy(): Promise<void> {
    await this._mutex.lock(async () => {
      for (const key of this._items.keys()) {
        await this._destroyItem(key);
      }
    });
  }
}
