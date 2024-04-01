import { connect as natsConnect } from 'nats';
import { type ErrorCallback, type OvernatsGlobalOptions } from './types';
import { Backend } from './backend';
import { App } from './app';

export type ConnectOptions = {
  options?: Partial<OvernatsGlobalOptions>;
  uncaughtException?: ErrorCallback;
};

export async function connect(name: string, options?: ConnectOptions): Promise<App> {
  const {
    options: globalOptions,
    uncaughtException = console.error,
  } = options ?? {};
  
  const core = await natsConnect();
  const backend = new Backend({
    core,
    options: globalOptions,
    uncaughtException,
  });
  
  await backend.init();
  
  return new App({ backend, name });
}
