export type ScheduleCallback = (delay: number) => void;

export type SchedulerCallback = (now: number, schedule: ScheduleCallback) => Promise<void>;

export type SchedulerOptions = {
  callback: SchedulerCallback;
};

export class Scheduler {
  private _callback: SchedulerCallback;
  private _timeout: NodeJS.Timeout | undefined;
  private _destroyed: boolean;
  
  constructor(options: SchedulerOptions) {
    const {
      callback,
    } = options;
    
    this._tick = this._tick.bind(this);
    this.schedule = this.schedule.bind(this);
    
    this._callback = callback;
    this._timeout = undefined;
    this._destroyed = false;
  }
  
  private _tick(): void {
    this._timeout = undefined;
    this._callback(Date.now(), this.schedule);
  }
  
  public schedule(delay: number): void {
    if (this._timeout || this._destroyed) {
      return;
    }
    
    this._timeout = setTimeout(this._tick, delay);
  }
  
  public async destroy(): Promise<void> {
    if (this._timeout) {
      clearTimeout(this._timeout);
    }
    
    this._destroyed = true;
  }
}
