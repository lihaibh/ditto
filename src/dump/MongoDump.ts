import * as tar from "tar-stream";
import { MongoClient, Collection } from "mongodb";
import { Observable, Observer, concat, of, throwError, defer } from "rxjs";
import { finalize, filter, map, catchError, switchMapTo } from "rxjs/operators";
import * as fs from "fs";
import * as zlib from "zlib";
import * as moment from "moment";
import * as path from "path";
import {
  CollectionPackEvent,
  DumpEvent,
  DumpEventTypes,
  DumpSummary,
} from "./contracts";
import { promisify } from "util";
import * as joi from "joi";
import { MongoDumpSchemaError } from "./errors";
import { get } from "lodash";

import {
  MongoDumpError,
  MongoDumpPackCollectionError,
  MongoDumpInitializationError,
} from "./errors";
import {
  DumpProgress,
  MongoDumpOpts,
  CollectionPackProgress,
} from "./contracts";
import { eachValueFrom } from "rxjs-for-await";
import { CollectionPackState, CollectionPackQueued } from "./contracts";

const exists = promisify(fs.exists);
const stat = promisify(fs.lstat);
const unlink = promisify(fs.unlink);

const DEFAULT_BATCH_SIZE = 50;

/**
 * Creates a snapshot of a database and stream it׳s content into a specified location.
 * During the process the module reads data from the target database and emit events that you can subscribe to.
 */
export class MongoDump implements AsyncIterable<DumpEvent> {
  public opts: MongoDumpOpts;

  constructor(opts: MongoDumpOpts) {
    this.opts = opts;
  }

  [Symbol.asyncIterator]() {
    return this.iterator();
  }

  /**
   * Allows an easier iteration across the dump events.
   * Lazingly start the dump operation.
   * As long as there was no execution of "for await ..." expression the dump process wont start.
   *
   * @example
   * ```js
   * // by selecting specific event type
   * const dump = new MongoDump(opts);
   *
   * for await (const process of dump.iterator('process')) {
   *    console.log(process);
   * }
   *
   * // by listening to all kind of events
   * for await (const dumpEvent of dump.iterator()) {
   *    console.log(dumpEvent);
   * }
   * ```
   *
   * @param event an event fired during the dump process.
   */
  iterator<K extends DumpEventTypes>(
    event: K
  ): AsyncIterableIterator<DumpEvent[K]>;
  iterator(): AsyncIterableIterator<DumpEvent>;
  iterator<K extends DumpEventTypes>(event?: K) {
    if (event) {
      return eachValueFrom(this.dump$(event));
    }

    return eachValueFrom(this.dump$());
  }

  /**
   * Returns a promise that allows you to await until the dump operation is done.
   * The promise might be rejected if there was a critical error.
   * If the promise was fullfilled it will provide a summary details of the dump operation.
   */
  async promise() {
    return this.dump$()
      .pipe(
        filter((dump_event) => "progress" in dump_event),
        map((dump_event) => (dump_event as any).progress as DumpProgress),
        map(
          ({ overall, collections }) =>
            ({ overall, collections } as DumpSummary)
        )
      )
      .toPromise();
  }

  /**
   * Returns a stream of dump events to listen to during the dump process.
   *
   * @param event an event name to listen to during the dump process
   */
  dump$<K extends DumpEventTypes>(event: K): Observable<DumpEvent[K]>;
  dump$(): Observable<DumpEvent>;
  dump$<K extends DumpEventTypes>(event?: K) {
    return dump$(this.opts, event);
  }
}

function dump$<K extends DumpEventTypes>(opts: MongoDumpOpts, event?: K) {
  const target_path = path.resolve(
    process.cwd(),
    get(opts, "target.path") || ""
  );
  let isTargetPathValidated = false;
  let cleanupOnFailure = get(opts, "target.cleanupOnFailure") || false;

  const source$: Observable<DumpEvent> = Observable.create(
    async (observer: Observer<DumpEvent>) => {
      try {
        validateOpts(opts);

        const start_date = moment.utc();

        await validateFilePath(target_path);

        isTargetPathValidated = true;

        const pack = tar.pack();
        const tar_write_stream = fs.createWriteStream(target_path);
        const gzip_stream = zlib.createGzip(opts.target.gzip);

        const client = new MongoClient(opts.source.uri, {
          connectTimeoutMS: opts.source.connectTimeoutMS,
          raw: true,
          useNewUrlParser: true,
          useUnifiedTopology: true,
        });

        await client.connect();

        const db = client.db(opts.source.dbname);
        const collections = (await db.collections()).filter(
          (collection) => !collection.collectionName.startsWith("system.")
        );

        const collections_state: {
          [name: string]: CollectionPackState;
        } = collections.reduce((acc, collection) => {
          acc[collection.collectionName] = {
            status: "queued",
            name: collection.collectionName,
          };

          return acc;
        }, {} as { [collectionName: string]: CollectionPackQueued });

        const collections2pack$ = concat(
          ...collections.map((collection) => {
            // make sure the packing occur one after the other (waterfally)
            return collection2pack$(pack, collection, opts).pipe(
              map((collection_pack_event) => {
                return {
                  progress: collection_pack_event,
                  error: undefined,
                };
              }),
              catchError((error: MongoDumpError) => {
                return of({
                  progress: undefined,
                  error,
                  name: collection.collectionName,
                });
              })
            );
          })
        ).pipe(
          finalize(() => {
            pack.finalize();
          })
        );

        const pack2tarStream = pack.pipe(gzip_stream).pipe(tar_write_stream);

        pack2tarStream.on("error", (err) => {
          observer.next({
            progress: undefined,
            error: new MongoDumpError(err),
          });
        });

        pack2tarStream.on("close", async () => {
          // make sure connection is closed
          if (client.isConnected()) {
            await client.close();
          }

          observer.complete();
        });

        const collection2pack_subscription = collections2pack$.subscribe(
          (packing) => {
            if (packing.error) {
              collections_state[packing.name] = {
                status: "failed",
                name: packing.name,
                error: packing.error,
              };

              observer.next({ progress: undefined, error: packing.error });
            } else if (packing.progress) {
              const curr_date = moment.utc();
              const duration = curr_date.diff(start_date, "milliseconds");

              collections_state[packing.progress.name] = {
                ...packing.progress,
                status:
                  packing.progress.read_bytes === packing.progress.total_bytes
                    ? "completed"
                    : "inProgress",
              };

              const read_collections = Object.values(collections_state).filter(
                (state) => state.status !== "queued"
              );

              const { total_bytes, read_bytes } = Object.values(
                collections_state
              ).reduce(
                (acc, collection_state) => {
                  if (
                    collection_state.status === "inProgress" ||
                    collection_state.status === "completed"
                  ) {
                    acc.total_bytes += collection_state.total_bytes;
                    acc.read_bytes += collection_state.read_bytes;
                  }

                  return acc;
                },
                {
                  total_bytes: 0,
                  read_bytes: 0,
                }
              );

              const ratio_byte_read_per_second = Math.ceil(
                read_bytes /
                  moment.duration(duration, "milliseconds").asSeconds()
              );

              observer.next({
                error: undefined,
                progress: {
                  current: packing.progress,
                  collections: Object.values(collections_state),
                  overall: {
                    total_bytes,
                    read_bytes,
                    start_date: start_date.toDate(),
                    last_date: curr_date.toDate(),
                    duration,
                    ratio_byte_read_per_second,
                    total_collections: collections.length,
                    read_collections: read_collections.length,
                  },
                },
              });
            }
          },
          (error) => {
            observer.error(error);
          }
        );

        // tear down function (when unsubscribe from the observable)
        return () => {
          // stop all the streams
          pack2tarStream.end();
          collection2pack_subscription.unsubscribe();
        };
      } catch (error) {
        observer.error(new MongoDumpInitializationError(error));
      }
    }
  );

  let result$ = source$;

  if (event) {
    result$ = source$.pipe(
      map((dumpEvent) => dumpEvent[event]),
      filter((dumpEvent) => !!dumpEvent)
    ) as any;
  }

  return result$.pipe(
    catchError((error) => {
      return defer(async () => {
        if (
          cleanupOnFailure &&
          isTargetPathValidated &&
          (await exists(target_path)) &&
          (await stat(target_path)).isFile()
        ) {
          console.warn(`removing: "${target_path}"`);
          await unlink(target_path);
        }
      }).pipe(switchMapTo(throwError(error)));
    })
  );
}

function collection2pack$(
  pack: tar.Pack,
  collection: Collection,
  { source: { batchSize } }: MongoDumpOpts
): Observable<CollectionPackEvent> {
  return Observable.create(async (observer: Observer<CollectionPackEvent>) => {
    function handleError(error: MongoDumpPackCollectionError) {
      observer.error(error);
    }

    const start_date = moment.utc();
    const collection_name = collection.collectionName;
    const pack_bson_filename = `${collection_name}.bson`;

    try {
      const collection_stream = collection
        .find()
        .batchSize(batchSize || DEFAULT_BATCH_SIZE);
      const collection_indexes = await collection.indexes();
      const collection_stats = await collection.stats();
      const collection_size = collection_stats.size;

      const total_bytes = collection_size;
      let read_bytes = 0;
      let is_data_delivered = false;

      const entry = pack.entry(
        { name: pack_bson_filename, size: collection_size },
        function (write_collection_bson_error) {
          if (write_collection_bson_error) {
            handleError(
              new MongoDumpPackCollectionError(
                write_collection_bson_error,
                collection_name,
                pack_bson_filename
              )
            );
          } else {
            const pack_metadata_filename = `${collection_name}.metadata.json`;
            const collection_metadata = JSON.stringify({
              options: {},
              indexes: collection_indexes,
              uuid: "",
            });

            // synchroniously packing collection metadata
            pack.entry(
              { name: pack_metadata_filename },
              collection_metadata,
              (write_metadata_error) => {
                if (write_metadata_error) {
                  handleError(
                    new MongoDumpPackCollectionError(
                      write_metadata_error,
                      collection_name,
                      pack_metadata_filename
                    )
                  );
                }
              }
            );
          }
        }
      );

      // asynchroniously packing collection data
      collection_stream.on("end", entry.end.bind(entry));

      collection_stream.on("data", (chunk: Uint8Array) => {
        is_data_delivered = true;
        read_bytes += chunk.length;

        const curr_date = moment.utc();
        const duration = curr_date.diff(start_date, "milliseconds");
        const ratio_byte_read_per_second = Math.ceil(
          read_bytes / moment.duration(duration, "milliseconds").asSeconds()
        );

        observer.next({
          chunk,
          name: collection_name,
          total_bytes,
          read_bytes,
          start_date: start_date.toDate(),
          last_date: curr_date.toDate(),
          duration,
          stats: collection_stats,
          ratio_byte_read_per_second,
        });
      });

      const collection_to_pack_stream = collection_stream
        .pipe(entry)
        .on("finish", () => {
          if (!is_data_delivered) {
            const curr_date = moment.utc();
            const duration = curr_date.diff(start_date, "milliseconds");
            const ratio_byte_read_per_second = Math.ceil(
              read_bytes / moment.duration(duration, "milliseconds").asSeconds()
            );

            observer.next({
              chunk: new Uint8Array(),
              name: collection_name,
              total_bytes,
              read_bytes,
              start_date: start_date.toDate(),
              last_date: curr_date.toDate(),
              duration,
              stats: collection_stats,
              ratio_byte_read_per_second,
            });
          }

          observer.complete();
        });

      return () => {
        collection_to_pack_stream.end();
      };
    } catch (error) {
      handleError(
        new MongoDumpPackCollectionError(
          error,
          collection_name,
          pack_bson_filename
        )
      );
    }
  });
}

async function validateFilePath(target_path: string) {
  const folder_path = path.dirname(target_path);

  if (await exists(target_path)) {
    throw new Error(`path to target file: "${target_path}" must not exists`);
  }

  if (
    !((await exists(folder_path)) && (await stat(folder_path)).isDirectory())
  ) {
    throw new Error(`directory: "${folder_path}" is not valid`);
  }
}

function validateOpts(opts: MongoDumpOpts) {
  const opts_schema = joi.object({
    source: joi
      .object({
        uri: joi.string().required(),
        dbname: joi.string().required(),
        batchSize: joi.number().optional(),
        connectTimeoutMS: joi.number().optional(),
      })
      .required(),
    target: joi
      .object({
        path: joi.string().required(),
        gzip: joi
          .object({
            flush: joi.number().optional(),
            finishFlush: joi.number().optional(),
            chunkSize: joi.number().optional(),
            windowBits: joi.number().optional(),
            level: joi.number().optional(),
            memLevel: joi.number().optional(),
            strategy: joi.number().optional(),
          })
          .optional(),
        cleanupOnFailure: joi.boolean().optional(),
      })
      .required(),
  });

  const { error } = opts_schema.validate(opts, { abortEarly: true });

  if (error) {
    throw new MongoDumpSchemaError(error);
  }
}
