import { Container } from 'ominous';
import { Autodestructible } from './autodestructible';
import { type Core } from './core';
import { Client } from './client';

export type AppOptions = {
  core: Core;
};

export class App extends Autodestructible {
  private _core: Core;
  
  constructor(options: AppOptions) {
    super();
    
    const {
      core,
    } = options;
    
    this._core = this.use(core);
  }
  
  public client(name: string): Client {
    const client = new Client({
      telemetry: this._core.telemetry.child(name),
      core: this._core,
      name,
      container: new Container(),
    });
    
    return this.use(client);
  }
  
  public get core(): Core {
    return this._core;
  }
}
