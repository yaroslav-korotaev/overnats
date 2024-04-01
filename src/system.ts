import { Autodestructible } from './autodestructible';
import { type Backend } from './backend';
import { Client } from './client';

export type SystemOptions = {
  backend: Backend;
};

export class System extends Autodestructible {
  private _backend: Backend;
  
  constructor(options: SystemOptions) {
    super();
    
    const {
      backend,
    } = options;
    
    this._backend = this.use(backend);
  }
  
  public client(name?: string): Client {
    const client = new Client({
      backend: this._backend,
      name: name || 'default',
    });
    
    return this.use(client);
  }
}
