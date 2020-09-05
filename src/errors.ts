import { VError } from "verror";

export declare interface MongoTransferError {
  new(message: string, cause?: Error): MongoTransferError;
}

export class MongoTransferError extends VError {
  constructor(cause: Error, message?: string) {
    super(message ?? `could not transfer mongo database properly`, cause);
  }
}

export class ConnectorSchemaError extends MongoTransferError {
  constructor(cause: Error) {
    super(cause);
  }
}
