import { Autodestructible } from './autodestructible';
import { type Backend } from './backend';
import { Client } from './client';

export type AppOptions = {
  backend: Backend;
  name: string;
};

export class App extends Autodestructible {
  private _backend: Backend;
  private _name: string;
  
  constructor(options: AppOptions) {
    super();
    
    const {
      backend,
      name,
    } = options;
    
    this._backend = this.use(backend);
    this._name = name;
  }
  
  public client(name?: string): Client {
    const client = new Client({
      backend: this._backend,
      name: `${this._name}.${name || 'default'}`,
    });
    
    return this.use(client);
  }
  
  public get backend(): Backend {
    return this._backend;
  }
}
