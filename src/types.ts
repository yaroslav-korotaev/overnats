import { type ErrorCallback } from 'ominous';
import { type StreamOptions } from './stream';

export type Headers = Record<string, string | string[]>;

export type Trapdoor = {
  uncaughtException: ErrorCallback;
};

export type OvernatsGlobalOptions = {
  heartbeatInterval: number;
  lockTimeout: number;
  streamDefaults?: Partial<StreamOptions>;
};

export type SubscribeParams<P> = {
  params: P;
};

export type SubscribeResult = {
  stream: string;
};
