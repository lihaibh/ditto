import { VError } from "verror";

export declare interface MongoDumpError {
  new (message: string, cause?: Error): MongoDumpError;
}

export class MongoDumpError extends VError {
  constructor(cause: Error, message?: string) {
    super(message ?? `could not dump mongo database properly`, cause);
  }
}

export class MongoDumpPackCollectionError extends MongoDumpError {
  constructor(cause: Error, collection_name: string, file_name: string) {
    super(
      cause,
      `could not pack collection: "${collection_name}" to file: "${file_name}"`
    );
  }
}

export class MongoDumpSchemaError extends MongoDumpError {
  constructor(cause: Error) {
    super(cause);
  }
}

export class MongoDumpInitializationError extends MongoDumpError {
  constructor(cause: Error) {
    super(cause, `could not construct MongoDB dump stream`);
  }
}
