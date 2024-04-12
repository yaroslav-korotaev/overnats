import crypto from 'node:crypto';
import stringify from 'fast-json-stable-stringify';
import { customAlphabet } from 'nanoid';
import {
  type MsgHdrs,
  NatsError,
  headers as createHeaders,
} from 'nats';
import { type Headers } from './types';

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

export function toHeaders(headers: Headers): MsgHdrs {
  const result = createHeaders();
  const keys = Object.keys(headers);
  
  for (const key of keys) {
    const value = headers[key];
    
    if (Array.isArray(value)) {
      for (const item of value) {
        result.append(key, item);
      }
    } else {
      result.set(key, value);
    }
  }
  
  return result;
}

export function fromHeaders(headers: MsgHdrs): Headers {
  const result: Headers = {};
  const keys = headers.keys();
  
  for (const key of keys) {
    const values = headers.values(key);
    
    if (values.length == 1) {
      result[key] = values[0];
    } else {
      result[key] = values;
    }
  }
  
  return result;
}
