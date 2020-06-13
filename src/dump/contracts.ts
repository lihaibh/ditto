import { CollStats } from "mongodb";
import { ZlibOptions } from "zlib";
import { MongoDumpError } from "./errors";

export interface CompressOptions {}

export interface MongoDumpOpts {
  source: {
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
    /**
     * The number of objects to fetch for each batch from the source.
     * The bigger this number is, it will provide better performance but will cost a higher memory usage.
     *
     * @default 50
     */
    batchSize?: number;
  };
  target: {
    /**
     * The path to the archive file to create (relative to cwd).
     */
    path: string;
    /**
     * compression options
     */
    gzip?: Omit<ZlibOptions, "dictionary">;
    /**
     * whether or not performing a cleanup of the target in case of an error
     *
     * @default false
     */
    cleanupOnFailure?: boolean;
  };
}

export interface CollectionDetails {
  name: string;
  stats: CollStats;
}

export type CollectionPackEvent = CollectionPackProgress & {
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

export type CollectionPackProgress = Progress & CollectionDetails;

export type CollectionPackQueued = {
  status: "queued";
  name: string;
};
export type CollectionPackFailed = {
  status: "failed";
  name: string;
  error: MongoDumpError;
};
export type CollectionPackCompleted = CollectionPackProgress & {
  status: "completed";
};
export type CollectionPackInProgress = CollectionPackProgress & {
  status: "inProgress";
};

export type CollectionPackState =
  | CollectionPackInProgress
  | CollectionPackCompleted
  | CollectionPackFailed
  | CollectionPackQueued;

export interface DumpProgress {
  overall: Progress & {
    total_collections: number;
    read_collections: number;
  };
  collections: CollectionPackState[];
  current: CollectionPackProgress & { chunk: Uint8Array };
}

export type DumpSummary = Omit<DumpProgress, "current">;

type DumpProgressEvent = { progress: DumpProgress; error: undefined };
type DumpErrorEvent = { progress: undefined; error: MongoDumpError };

export type DumpEvent = DumpProgressEvent | DumpErrorEvent;
export type DumpEventTypes = keyof DumpEvent;
