import crypto from 'node:crypto';
import stringify from 'fast-json-stable-stringify';
import { customAlphabet } from 'nanoid';
import { NatsError } from 'nats';
import { type AsyncCallback, type ErrorCallback, type Trapdoor } from './types';

export class Mutex {
  private _queue: AsyncCallback[];
  private _busy: boolean;
  
  constructor() {
    this._queue = [];
    this._busy = false;
    this._next = this._next.bind(this);
  }
  
  private _next(): void {
    if (this._queue.length == 0) {
      this._busy = false;
      return;
    }
    
    this._busy = true;
    const callback = this._queue.shift()!;
    callback().then(this._next);
  }
  
  public async lock(callback: AsyncCallback): Promise<void> {
    return new Promise((resolve, reject) => {
      this._queue.push(async () => {
        try {
          await callback();
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      
      if (!this._busy) {
        this._next();
      }
    });
  }
}

export function hashOf(value: unknown | undefined): string {
  if (value == undefined) {
    return '00000000000000000000000000000000';
  }
  
  const json = stringify(value);
  const hash = crypto.createHash('md5').update(json).digest('hex');
  
  return hash;
}

export const NANOID_ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
export const nanoid = customAlphabet(NANOID_ALPHABET, 24);

export function distribute(
  instances: string[],
  shards: string[],
  replicas: number,
): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  const list: string[][] = [];
  
  for (const key of instances) {
    const instance: string[] = [];
    
    result[key] = instance;
    list.push(instance);
  }
  
  for (const shard of shards) {
    const candidates = list.filter(instance => {
      return !instance.includes(shard);
    });
    const count = Math.min(replicas, candidates.length);
    
    for (let i = 0; i < count; i++) {
      candidates.sort((a, b) => a.length - b.length);
      candidates.shift()!.push(shard);
    }
  }
  
  return result;
}

export function digitsOfBy(number: number, base: number): number {
  let count = 0;
  
  do {
    count++;
    number = Math.floor(number / base);
  } while (number > 0);
  
  return count;
}

export function toStringWithAlphabet(number: number, alphabet: string): string {
  let result = '';
  
  do {
    result = alphabet[number % alphabet.length] + result;
    number = Math.floor(number / alphabet.length);
  } while (number > 0);
  
  return result;
}

export const SEQUENCE_ALPHABET = 'abcdefghijklmnopqrstuvwxyz';

export function sequence(length: number): string[] {
  if (length == 0) {
    return [];
  }
  
  const alphabet = SEQUENCE_ALPHABET;
  const result: string[] = [];
  const digits = digitsOfBy(length - 1, alphabet.length);
  
  for (let i = 0; i < length; i++) {
    const value = toStringWithAlphabet(i, alphabet).padStart(digits, alphabet[0]);
    result.push(value);
  }
  
  return result;
}

export function isWrongLastSequenceError(err: unknown): boolean {
  return !!(err && err instanceof NatsError && err.api_error && err.api_error.err_code == 10071);
}

export async function tryWithFallback<T>(
  callback: () => Promise<T>,
  fallback: (err: unknown) => Promise<T>,
): Promise<T> {
  try {
    return await callback();
  } catch (err) {
    return await fallback(err);
  }
}

export async function anyway<T>(
  callback: () => Promise<T>,
  cleanup: () => Promise<void>,
): Promise<T> {
  let result: T;
  
  try {
    result = await callback();
  } catch (err) {
    await cleanup();
    
    throw err;
  }
  
  await cleanup();
  
  return result;
}

export type RetryOptions = {
  when: (err: unknown, retry: number) => boolean;
  retries: number;
  signal?: AbortSignal;
  minDelay: number;
  maxDelay: number;
  factor: number;
  jitter: number;
};

export async function retry<T>(
  callback: () => Promise<T>,
  options?: Partial<RetryOptions>,
): Promise<T> {
  const {
    when = () => true,
    retries = 10,
    signal,
    minDelay = 250,
    maxDelay = 120_000,
    factor = 1.5,
    jitter = 0.1,
  } = options ?? {};
  
  let abort: ErrorCallback | undefined = undefined;
  let alive = true;
  let retry = 0;
  
  if (signal) {
    signal.addEventListener('abort', event => {
      if (abort) {
        abort(signal.reason ?? new Error('aborted'));
      }
      
      alive = false;
    });
  }
  
  while (true) {
    try {
      return await callback();
    } catch (err) {
      if (alive && retry < retries && when(err, retry)) {
        const raw = minDelay * Math.pow(factor, retry);
        const limited = Math.min(raw, maxDelay);
        const jittered = limited * (1 - jitter + Math.random() * (jitter * 2));
        const delay = Math.round(jittered);
        
        await new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            abort = undefined;
            resolve();
          }, delay);
          
          abort = err => {
            clearTimeout(timeout);
            reject(err);
          };
        });
        
        retry++;
      } else {
        throw err;
      }
    }
  }
}

export function rootCause(err: unknown): unknown {
  while (err instanceof Error && err.cause) {
    err = err.cause;
  }
  
  return err;
}

export type CompareCallback<T> = (a: T, b: T) => boolean;

export function defaultCompare<T>(a: T, b: T): boolean {
  return hashOf(a) == hashOf(b);
}
