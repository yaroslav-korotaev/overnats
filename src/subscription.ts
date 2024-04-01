import { type Subscription as NatsSubscription, Msg as NatsMsg } from 'nats';
import { OvernatsError } from './errors';
import { type Listener } from './listener';
import { type Backend } from './backend';

export type SubscriptionBackendOptions = {
  queue?: string | undefined;
};

export type SubscriptionMessageRespondCallback = (data?: unknown) => void;

export type SubscriptionMessage<T> = {
  subject: string;
  data: T;
  respond: SubscriptionMessageRespondCallback;
};

export type SubscriptionCallback<T> = (message: SubscriptionMessage<T>) => Promise<void>;

export type SubscriptionOptions<T> = {
  backend: Backend;
  subject: string;
  options?: SubscriptionBackendOptions;
  callback: SubscriptionCallback<T>;
};

export class Subscription<T> {
  private _backend: Backend;
  private _subject: string;
  private _options?: SubscriptionBackendOptions;
  private _callback: SubscriptionCallback<T>;
  private _subscription!: NatsSubscription;
  private _subscriptionListener!: Listener<NatsMsg>;
  
  constructor(options: SubscriptionOptions<T>) {
    const {
      backend,
      subject,
      options: backendOptions,
      callback,
    } = options;
    
    this._backend = backend;
    this._subject = subject;
    this._options = backendOptions;
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    this._subscription = this._backend.core.subscribe(this._subject, this._options);
    this._subscriptionListener = await this._backend.listen(
      `subscription.${this._subject}`,
      this._subscription,
      async message => {
        try {
          await this._callback({
            subject: message.subject,
            data: this._backend.decode(message.data) as T,
            respond: data => {
              message.respond(this._backend.encode(data));
            },
          });
        } catch (err) {
          throw new OvernatsError('subscription error', {
            cause: err,
            details: {
              subject: this._subject,
            },
          });
        }
      },
    );
  }
  
  public async destroy(): Promise<void> {
    await this._subscription.drain();
    await this._subscriptionListener.destroy();
  }
}
