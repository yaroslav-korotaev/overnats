import { type Destructible, Mutex } from 'ominous';
import { type CompareCallback, defaultCompare } from './utils';

export type SummonCallback<P> = (params: P) => Promise<Destructible>;

export type SummonerMoreOptions<P> = {
  compare?: CompareCallback<P>;
};

export type SummonerOptions<P> = SummonerMoreOptions<P> & {
  callback: SummonCallback<P>;
};

export class Summoner<P> {
  private _compare: CompareCallback<P>;
  private _callback: SummonCallback<P>;
  private _mutex: Mutex;
  private _ghost: Ghost<P> | undefined;
  
  constructor(options: SummonerOptions<P>) {
    const {
      compare = defaultCompare,
      callback,
    } = options;
    
    this._compare = compare;
    this._callback = callback;
    this._mutex = new Mutex();
    this._ghost = undefined;
  }
  
  private async _spawn(params: P): Promise<void> {
    const object = await this._callback(params);
    this._ghost = { params, object };
  }
  
  private async _kill(): Promise<void> {
    if (!this._ghost) {
      return;
    }
    
    await this._ghost.object.destroy();
    this._ghost = undefined;
  }
  
  private async _maybeRespawn(params: P): Promise<void> {
    if (this._ghost) {
      if (this._compare(params, this._ghost.params)) {
        return;
      }
      
      await this._kill();
    }
    
    await this._spawn(params);
  }
  
  public async spawn(params: P): Promise<void> {
    await this._mutex.lock(async () => {
      await this._maybeRespawn(params);
    });
  }
  
  public async kill(): Promise<void> {
    await this._mutex.lock(async () => {
      await this._kill();
    });
  }
  
  public async destroy(): Promise<void> {
    await this._mutex.lock(async () => {
      await this._kill();
    });
  }
}

type Ghost<P> = {
  params: P;
  object: Destructible;
}
