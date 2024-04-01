import { serializeError } from './errors';
import { Autodestructible } from './autodestructible';
import { type Backend } from './backend';
import { type SubscriptionMessage } from './subscription';

export type ServiceMethodRequest<T> = {
  data: T;
};

export type ServiceMethodResponse<T> = {
  data: T;
};

export type ServiceMethodContext<P, R> = {
  req: ServiceMethodRequest<P>;
  res: ServiceMethodResponse<R>;
};

export type ServiceMethodCallback<P, R> = (ctx: ServiceMethodContext<P, R>) => Promise<void>;

export type ServiceOptions = {
  backend: Backend;
  name: string;
};

export class Service extends Autodestructible {
  private _backend: Backend;
  private _name: string;
  
  constructor(options: ServiceOptions) {
    super();
    
    const {
      backend,
      name,
    } = options;
    
    this._backend = backend;
    this._name = name;
  }
  
  public async method<P, R>(name: string, callback: ServiceMethodCallback<P, R>): Promise<void> {
    const subject = `${this._name}.${name}`;
    const subscription = await this._backend.subscribe<P>(subject, async message => {
      const ctx = context<P, R>(message);
      
      try {
        await callback(ctx);
        
        message.respond({ result: ctx.res.data });
      } catch (err) {
        message.respond({ error: serializeError(err) });
      }
    }, { queue: subject });
    
    this.use(subscription);
  }
}

function context<P, R>(message: SubscriptionMessage<P>): ServiceMethodContext<P, R> {
  return {
    req: { data: message.data },
    res: { data: undefined as R },
  };
};
