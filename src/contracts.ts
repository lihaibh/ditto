import { ZlibOptions } from "zlib";
import { MongoTransferError } from "./errors";

export type GzipOpts = Omit<ZlibOptions, "dictionary">;

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

export interface CollectionMetadata {
  name: string;
  size: number;
  indexes: any[];
}
