import { type CallbackSync } from 'ominous';
import { type QueuedIterator, type KvEntry, type KV } from 'nats';
import { OvernatsError } from './errors';
import { type Listener } from './listener';
import { type Core } from './core';

export type UpdatePut<T> = {
  operation: 'PUT',
  revision: number,
  online: boolean,
  key: string,
  value: T,
};

export type UpdateDelete = {
  operation: 'DEL',
  revision: number,
  online: boolean,
  key: string,
};

export type Update<T> = UpdatePut<T> | UpdateDelete;

export type WatcherCallback<T> = (update: Update<T>) => Promise<void>;

export type WatcherOptions<T> = {
  core: Core;
  kv: KV,
  filter?: string;
  callback: WatcherCallback<T>;
};

export class Watcher<T> {
  private _core: Core;
  private _kv: KV;
  private _filter?: string;
  private _callback: WatcherCallback<T>;
  private _updates!: QueuedIterator<KvEntry>;
  private _updatesListener!: Listener<KvEntry>;
  
  constructor(options: WatcherOptions<T>) {
    const {
      core,
      kv,
      filter,
      callback,
    } = options;
    
    this._core = core;
    this._kv = kv;
    this._filter = filter;
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    let online = false;
    let resume: CallbackSync | undefined;
    
    this._updates = await this._kv.watch({
      key: this._filter,
      initializedFn: () => {
        online = true;
        
        if (resume) {
          resume();
        }
      },
    });
    this._updatesListener = await this._core.listen('watcher', this._updates, async entry => {
      try {
        const update = entryToUpdate<T>(this._core, entry, online);
        
        await this._callback(update);
      } catch (err) {
        throw new OvernatsError('watch error', { cause: err });
      }
    });
    
    if (!online) {
      await new Promise<void>(resolve => {
        resume = resolve;
      });
    }
  }
  
  public async destroy(): Promise<void> {
    this._updates.stop();
    await this._updatesListener.destroy();
  }
}

function entryToUpdate<T>(core: Core, entry: KvEntry, online: boolean): Update<T> {
  if (entry.operation == 'PUT') {
    return {
      operation: 'PUT',
      revision: entry.revision,
      online,
      key: entry.key,
      value: core.decode(entry.value) as T,
    };
  }
  
  return {
    operation: 'DEL',
    revision: entry.revision,
    online,
    key: entry.key,
  };
}
