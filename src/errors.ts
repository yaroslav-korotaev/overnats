import { DetailsErrorOptions, DetailsError, isDetailsErrorLike } from 'erroar';

export type OvernatsErrorOptions = DetailsErrorOptions;

export class OvernatsError extends DetailsError {}

export type OvernatsClientErrorOptions = OvernatsErrorOptions;

export class OvernatsClientError extends OvernatsError {}

export function error(
  message?: string,
  options?: OvernatsClientErrorOptions,
): OvernatsClientError {
  return new OvernatsClientError(message, options);
}

export function serializeError(err: unknown): unknown {
  if (err instanceof OvernatsClientError) {
    return {
      code: 'EFAIL',
      message: err.message,
      details: err.details,
    };
  }
  
  if (err instanceof OvernatsError) {
    return {
      code: 'EINTERNAL',
      message: err.message,
      details: err.details,
    };
  }
  
  return {
    code: 'EINTERNAL',
    message: 'internal error',
  };
}

export function deserializeError(value: unknown): OvernatsError {
  if (isDetailsErrorLike(value) && 'code' in value && typeof value.code === 'string') {
    if (value.code == 'EFAIL') {
      return new OvernatsClientError(value.message, {
        cause: value.cause,
        details: value.details,
      });
    }
    
    return new OvernatsError(value.message, { details: value.details });
  }
  
  return new OvernatsError('unknown error');
}
