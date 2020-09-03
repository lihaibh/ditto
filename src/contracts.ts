import { ZlibOptions } from "zlib";
import { MongoTransferError } from "./errors";

export type GzipOpts = Omit<ZlibOptions, "dictionary">;

export type MongoDBSource = MongoDBConnection & {
  /**
   * The number of objects to fetch for each batch from the source.
   * The bigger this number is, it will provide better performance but will consume more memory.
   *
   * @default 50
   */
  batchSize?: number;
}

export interface MongoDBConnection {
  /**
   * https://docs.mongodb.com/manual/reference/connection-string/
   * A uri contains credentials (if necessary) and the hostname or ip address.
   * this uri used in order to authenticate to the source MondoDB server.
   * example:
   * mongodb://username:password@host:27017
   */
  uri: string;
  /**
   * The name of the database to create a dump for.
   */
  dbname: string;
  /**
  * The time in milliseconds to attempt a connection before timing out.
  */
  connectTimeoutMS?: number;
}

export interface LocalFilesystemConnection {
  /**
   * The path to the archive file to create (relative to current working directory).
   */
  path: string;
}

export type TargetFileConnection = LocalFilesystemConnection;

export interface CollectionDetails {
  metadata: CollectionMetadata
}

export type CollectionTransferEvent = CollectionTransferProgress & {
  chunk: Uint8Array;
};

export interface Progress {
  total_bytes: number;
  read_bytes: number;
  start_date: Date;
  last_date: Date;
  duration: number;
  ratio_byte_read_per_second: number;
}

export type CollectionTransferProgress = Progress & CollectionDetails;

export type CollectionTransferQueued = CollectionDetails & {
  status: "queued";
};

export type CollectionTransferFailed = CollectionDetails & {
  status: "failed";
  error: MongoTransferError;
};

export type CollectionTransferCompleted = CollectionTransferProgress & {
  status: "completed";
};

export type CollectionTransferInProgress = CollectionTransferProgress & {
  status: "inProgress";
};

export type CollectionTranferState =
  | CollectionTransferInProgress
  | CollectionTransferCompleted
  | CollectionTransferFailed
  | CollectionTransferQueued;

export interface TransferSummary {
  overall: Progress & {
    total_collections: number;
    read_collections: number;
  };
  collections: CollectionTranferState[];
}

export type TransferProgress = TransferSummary & {
  current: CollectionTransferProgress & { chunk: Uint8Array };
}

type TransferProgressEvent = { progress: TransferProgress; error: undefined };
type TransferErrorEvent = { progress: undefined; error: MongoTransferError };

export type TransferEvent = TransferProgressEvent | TransferErrorEvent;
export type TransferEventTypes = keyof TransferEvent;

export interface CollectionMetadata {
  name: string;
  size: number;
  indexes: any[];
}
