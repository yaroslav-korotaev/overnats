import { type Backend } from './backend';

export type TimerCallback = (now: number) => Promise<void>;

export type TimerOptions = {
  backend: Backend;
  interval: number;
  callback: TimerCallback;
};

export class Timer {
  private _backend: Backend;
  private _started: number;
  private _interval: number;
  private _callback: TimerCallback;
  private _timer: NodeJS.Timeout | undefined;
  private _destroyed: boolean;
  
  constructor(options: TimerOptions) {
    const {
      backend,
      interval,
      callback,
    } = options;
    
    this._schedule = this._schedule.bind(this);
    this._tick = this._tick.bind(this);
    
    this._backend = backend;
    this._started = Date.now();
    this._interval = interval;
    this._callback = callback;
    this._destroyed = false;
  }
  
  private _schedule(): void {
    if (this._destroyed) {
      return;
    }
    
    const now = Date.now();
    const elapsed = now - this._started;
    const timeout = this._interval - (elapsed % this._interval);
    
    this._timer = setTimeout(this._tick, timeout);
  }
  
  private _tick(): void {
    const now = Date.now();
    
    this._started = now;
    this._timer = undefined;
    
    this._callback(now)
      .then(this._schedule)
      .catch(err => {
        console.log('timer callback error catched', { err });
        
        this._backend.uncaughtException(err);
        this._schedule();
      })
    ;
  }
  
  public async init(): Promise<void> {
    this._schedule();
  }
  
  public async destroy(): Promise<void> {
    if (this._timer) {
      clearTimeout(this._timer);
    }
    
    this._destroyed = true;
  }
}
