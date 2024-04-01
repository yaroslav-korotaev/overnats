export type Callback = () => void;
export type AsyncCallback = () => Promise<void>;
export type UseCallback<T> = (value: T) => Promise<void>;
export type ReturnCallback<T> = () => Promise<T>;
export type MutateCallback<T> = (value: T | undefined) => Promise<T>;
export type MutateUsingCallback<T> = (value: T | undefined, use: UseCallback<T>) => Promise<void>;
export type ErrorCallback = (err: unknown) => void;

export type Trapdoor = {
  uncaughtException: ErrorCallback;
};

export type Destructible = {
  destroy(): Promise<void>;
};

export type OvernatsGlobalOptions = {
  heartbeatInterval: number;
  lockTimeout: number;
};

export type SubscribeParams<P> = {
  params: P;
};

export type SubscribeResult = {
  stream: string;
};
