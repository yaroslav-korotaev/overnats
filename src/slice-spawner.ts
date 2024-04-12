import { type Destructible, setup } from 'ominous';
import { type Spawner } from './spawner';
import { type BucketSlice } from './bucket';
import { type Watcher } from './watcher';
import { type Core } from './core';

export type SliceSpawnerCallbackParams<V> = {
  core: Core;
  name: string;
  key: string;
  value: V;
};

export type SliceSpawnerCallback<V, W extends Destructible> = (
  params: SliceSpawnerCallbackParams<V>,
) => Promise<W>;

export type SliceSpawnerParams<V, W extends Destructible> = {
  core: Core;
  name: string;
  slice: BucketSlice<V>;
  callback: SliceSpawnerCallback<V, W>;
};

export class SliceSpawner<V, W extends Destructible> {
  private _core: Core;
  private _name: string;
  private _slice: BucketSlice<V>;
  private _callback: SliceSpawnerCallback<V, W>;
  private _spawner!: Spawner<V, W>;
  private _watcher!: Watcher<V>;
  
  constructor(params: SliceSpawnerParams<V, W>) {
    const {
      core,
      name,
      slice,
      callback,
    } = params;
    
    this._core = core;
    this._name = name;
    this._slice = slice;
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      const spawner = await use(async () => {
        return await this._core.spawner<V, W>(async (key, value) => {
          return await this._callback({
            core: this._core,
            name: `${this._name}.${key}`,
            key,
            value,
          });
        });
      });
      
      const watcher = await use(async () => {
        return await this._slice.watch(async update => {
          if (update.operation == 'PUT') {
            await spawner.maybeRespawnItem(update.key, update.value);
          }
          
          if (update.operation == 'DEL') {
            await spawner.destroyItem(update.key);
          }
        }, { detach: true });
      });
      
      this._spawner = spawner;
      this._watcher = watcher;
    });
  }
  
  public async destroy(): Promise<void> {
    await this._watcher.destroy();
    await this._spawner.destroy();
  }
}

export type SliceSpawnerFactory = <V, W extends Destructible>(
  name: string,
  slice: BucketSlice<V>,
  callback: SliceSpawnerCallback<V, W>,
) => Promise<SliceSpawner<V, W>>;
