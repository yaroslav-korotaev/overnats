import { type Telemetry } from 'universe-types';
import { type ErrorCallback } from 'ominous';
import { connect as natsConnect } from 'nats';
import { type OvernatsGlobalOptions } from './types';
import { Core } from './core';
import { App } from './app';

export type ConnectParams = {
  telemetry: Telemetry;
  token?: string;
  username?: string;
  password?: string;
  servers?: string | string[];
  options?: Partial<OvernatsGlobalOptions>;
  uncaughtException?: ErrorCallback;
};

export async function connect(params: ConnectParams): Promise<App> {
  const {
    telemetry,
    token,
    username,
    password,
    servers,
    options: globalOptions,
    uncaughtException = console.error,
  } = params;
  
  const nats = await natsConnect({
    token,
    user: username,
    pass: password,
    servers,
  });
  const core = new Core({
    telemetry,
    nats,
    options: globalOptions,
    uncaughtException,
  });
  
  await core.init();
  
  return new App({ core });
}
