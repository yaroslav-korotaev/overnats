import { anyway } from './utils';

export type AbortableCallback<T> = (signal: AbortSignal) => Promise<T>;

export class Autoabortable {
  public _controllers: Set<AbortController>;
  
  constructor() {
    this._controllers = new Set();
  }
  
  public abort(err?: unknown): void {
    const reason = err ?? new AutoabortableError();
    for (const controller of this._controllers) {
      controller.abort(reason);
    }
  }
  
  public async execute<T>(callback: AbortableCallback<T>): Promise<T> {
    const controller = new AbortController();
    
    this._controllers.add(controller);
    
    return await anyway(async () => {
      return await callback(controller.signal);
    }, async () => {
      this._controllers.delete(controller);
    });
  }
}

export class AutoabortableError extends Error {
  constructor() {
    super('shutdown');
  }
}
