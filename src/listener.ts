import { type UseCallback, type Trapdoor } from './types';
import { OvernatsError } from './errors';

export type ListenerOptions<T> = {
  trapdoor: Trapdoor;
  name: string;
  iterable: AsyncIterable<T>;
  callback: UseCallback<T>;
};

export class Listener<T> {
  private _trapdoor: Trapdoor;
  private _name: string;
  private _iterable: AsyncIterable<T>;
  private _callback: UseCallback<T>;
  private _promise!: Promise<void>;
  
  constructor(options: ListenerOptions<T>) {
    const {
      trapdoor,
      name,
      iterable,
      callback,
    } = options;
    
    this._trapdoor = trapdoor;
    this._name = name;
    this._iterable = iterable;
    this._callback = callback;
  }
  
  private async _listen(): Promise<void> {
    try {
      for await (const value of this._iterable) {
        try {
          await this._callback(value);
        } catch (err) {
          this._trapdoor.uncaughtException(new OvernatsError('listener callback failed', {
            cause: err,
            details: { listener: this._name },
          }));
        }
      }
    } catch (err) {
      this._trapdoor.uncaughtException(new OvernatsError('listener failed', {
        cause: err,
        details: { listener: this._name },
      }));
    }
  }
  
  public async init(): Promise<void> {
    this._promise = this._listen();
  }
  
  public async destroy(): Promise<void> {
    await this._promise;
  }
}
