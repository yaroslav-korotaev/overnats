import {
  type ConsumerMessages,
  type JsMsg,
  type ConsumerStatus,
  ConsumerEvents,
} from 'nats';
import { type ErrorCallback, type SubscribeParams, type SubscribeResult } from './types';
import { OvernatsError } from './errors';
import { Autodestructible } from './autodestructible';
import { type Listener } from './listener';
import { type Summoner } from './summoner';
import { type Backend } from './backend';

export type ConsumerCallback<T> = (event: T) => Promise<void>;

export type ConsumerOptions<P, E> = {
  backend: Backend;
  name: string;
  producer: string;
  params: P;
  callback: ConsumerCallback<E>;
};

export class Consumer<P, E> extends Autodestructible {
  private _backend: Backend;
  private _name: string;
  private _producer: string;
  private _params: P;
  private _callback: ConsumerCallback<E>;
  private _summoner!: Summoner<SubscribeResult>;
  
  constructor(options: ConsumerOptions<P, E>) {
    super();
    
    const {
      backend,
      name,
      producer,
      params,
      callback,
    } = options;
    
    this._subscribe = this._subscribe.bind(this);
    this._spawn = this._spawn.bind(this);
    
    this._backend = backend;
    this._name = name;
    this._producer = producer;
    this._params = params;
    this._callback = callback;
  }
  
  private async _subscribe(): Promise<void> {
    try {
      await this._backend.retry(async () => {
        type Params = SubscribeParams<P>;
        type Result = SubscribeResult;
        const method = `producer.${this._producer}.subscribe`;
        const response = await this._backend.call<Params, Result>(method, {
          params: this._params,
        });
        
        await this._summoner.spawn(response);
      }, {
        retries: 2,
        minDelay: 1000,
        factor: 2,
      });
    } catch (err) {
      this._backend.uncaughtException(new OvernatsError('consumer heartbeat failed', {
        cause: err,
        details: { consumer: this._name },
      }));
      
      await this._summoner.kill();
    }
  }
  
  private async _spawn(params: SubscribeResult): Promise<ConsumerSubscription<E>> {
    const subscription = new ConsumerSubscription({
      backend: this._backend,
      name: this._name,
      params,
      callback: this._callback,
      error: err => {
        this._backend.uncaughtException(new OvernatsError('consumer subscription failed', {
          cause: err,
          details: { consumer: this._name },
        }));
        
        this._summoner.kill().catch(err => {
          this._backend.uncaughtException(new OvernatsError('consumer subscription kill failed', {
            cause: err,
            details: { consumer: this._name },
          }));
        });
      },
    });
    
    await subscription.init();
    
    return subscription;
  }
  
  public async init(): Promise<void> {
    this._summoner = this.use(await this._backend.summoner(this._spawn));
    
    await this._subscribe();
    
    const interval = this._backend.options.heartbeatInterval;
    const timerName = `consumer.${this._name}.heartbeat`;
    this.use(await this._backend.localTimer(timerName, interval, this._subscribe));
  }
}

export type ConsumerSubscriptionOptions<E> = {
  backend: Backend;
  name: string;
  params: SubscribeResult;
  callback: ConsumerCallback<E>;
  error: ErrorCallback;
};

export class ConsumerSubscription<E> {
  private _backend: Backend;
  private _name: string;
  private _params: SubscribeResult;
  private _callback: ConsumerCallback<E>;
  private _error: ErrorCallback;
  private _errorCalled: boolean;
  private _messages!: ConsumerMessages;
  private _messagesListener!: Listener<JsMsg>;
  private _statusesListener!: Listener<ConsumerStatus>;
  
  constructor(options: ConsumerSubscriptionOptions<E>) {
    const {
      backend,
      name,
      params,
      callback,
      error,
    } = options;
    
    this._backend = backend;
    this._name = name;
    this._params = params;
    this._callback = callback;
    this._error = error;
    this._errorCalled = false;
  }
  
  public async init(): Promise<void> {
    const info = await this._backend.jsConsumer(this._params.stream, this._name);
    const consumer = await this._backend.js.consumers.get(info.stream_name, info.name);
    
    this._messages = await consumer.consume();
    this._messagesListener = await this._backend.listen(
      `${this._name}.messages`,
      this._messages,
      async message => {
        const event = this._backend.decode(message.data) as E;
        
        try {
          await this._callback(event);
          message.ack();
        } catch (err) {
          const after = Math.min(1000 * Math.pow(2, message.info.redeliveryCount), 60_000);
          message.nak(after);
        }
      },
    );
    
    const statuses = await this._messages.status();
    this._statusesListener = await this._backend.listen(
      `${this._name}.statuses`,
      statuses,
      async status => {
        if (status.type == ConsumerEvents.HeartbeatsMissed) {
          const n = status.data as number;
          
          if (n == 2 && !this._errorCalled) {
            this._error(new Error('heartbeats missed'));
            this._errorCalled = true;
          }
        }
      },
    );
  }
  
  public async destroy(): Promise<void> {
    this._messages.stop();
    this._statusesListener.destroy();
    this._messagesListener.destroy();
  }
}
