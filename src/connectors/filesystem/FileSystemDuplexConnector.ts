import { TargetConnector, SourceConnector, WritableData } from "../Connector";
import { Readable, Writable } from "stream";
import { GzipOpts, CollectionMetadata } from "../../contracts";
import { Observable, Observer, concat, ReplaySubject, defer, fromEvent, throwError } from "rxjs";
import { groupBy, concatMap, toArray, map, take, first, switchMapTo, catchError, switchMap } from 'rxjs/operators';
import * as tar from "tar-stream";
import * as zlib from "zlib";
import { Validatable } from "../Validatable";

export interface FileSystemConnector {
    gzip: GzipOpts;
}

export interface FileSystemSourceConnector extends FileSystemConnector, SourceConnector {
    createReadStream(batch_size?: number): Readable;
}

export interface FileSystemTargetConnector extends FileSystemConnector, TargetConnector {
    createWriteStream(): Writable
}

interface PartialCollectionMetadata {
    name: string;
    size?: number;
    indexes?: any[];
}

export abstract class FileSystemDuplexConnector extends Validatable implements FileSystemSourceConnector, FileSystemTargetConnector {
    write(source: Observable<WritableData>, metadata: CollectionMetadata[]): Observable<number> {
        return write(this, source, metadata);
    }

    batch$(collection_name: string, batch_size: number): Observable<Buffer> {
        return new Observable((observer: Observer<Buffer>) => {
            const extract = tar.extract();

            const file_input_stream = this.createReadStream(batch_size);
            const unzip = zlib.createGunzip(this.gzip);

            extract.on('entry', ({ size, name }, source_stream, next) => {
                if (!name.endsWith('.bson')) {
                    // skipping
                    source_stream.resume();

                    return next();
                }

                const collection_name_in_tar = name.replace('.bson', '');

                if (collection_name !== collection_name_in_tar) {
                    // skipping
                    source_stream.resume();

                    return next();
                }

                source_stream.on('data', chunk => observer.next(chunk));
                source_stream.on('error', (error) => observer.error(error));
                source_stream.on('end', () => next());

                source_stream.resume();
            });

            extract.on('finish', () => {
                observer.complete();
            });

            // execute the stream
            file_input_stream.pipe(unzip).pipe(extract);
        });
    }

    async transferable(): Promise<CollectionMetadata[]> {
        return new Observable((observer: Observer<PartialCollectionMetadata>) => {
            const extract = tar.extract();

            const file_input_stream = this.createReadStream();
            const unzip = zlib.createGunzip(this.gzip);

            extract.on('entry', ({ size, name }, source_stream, next) => {
                if (!name.endsWith('.metadata.json')) {
                    if (name.endsWith('.bson')) {
                        const collection_name = name.replace('.bson', '');

                        observer.next({
                            name: collection_name,
                            size
                        });
                    }

                    source_stream.resume();

                    return next();
                }

                let metadata_str = '';
                const collection_name = name.replace('.metadata.json', '');

                source_stream.on('data', chunk => {
                    metadata_str += chunk.toString();
                });

                source_stream.on('error', (error) => observer.error(error));
                source_stream.on('end', () => {
                    const { indexes = [] } = JSON.parse(metadata_str);

                    observer.next({
                        name: collection_name,
                        indexes
                    });

                    next();
                });

            });

            extract.on('finish', () => {
                observer.complete();
            });

            // execute the stream
            file_input_stream.pipe(unzip).pipe(extract);
        }).pipe(
            groupBy(({ name }) => name, (partial_metadatas: PartialCollectionMetadata) => partial_metadatas, undefined, () => new ReplaySubject()),
            concatMap(metadataGroup$ =>
                metadataGroup$.pipe(
                    toArray(),
                    map(partial_metadatas =>
                        partial_metadatas.reduce((acc_metadata: CollectionMetadata, partial_metadata) =>
                            ({ ...acc_metadata, ...partial_metadata }), { name: metadataGroup$.key, size: 0, indexes: [] } as CollectionMetadata))
                ))
        ).pipe(
            toArray()
        ).toPromise();
    };

    abstract remove_on_failure: boolean;
    abstract remove_on_startup: boolean;
    abstract connection: any;
    abstract gzip: Pick<zlib.ZlibOptions, "flush" | "finishFlush" | "chunkSize" | "windowBits" | "level" | "memLevel" | "strategy">;
    abstract name: string;

    abstract createReadStream(batch_size?: number): Readable
    abstract createWriteStream(): Writable;

    abstract remove(): Promise<boolean>;

    abstract connect(): Promise<any>;
    abstract close(): Promise<any>;

    abstract exists(): Promise<boolean>;
    abstract fullname(): Promise<string>;
}

export function write(connector: FileSystemTargetConnector, data$: Observable<WritableData>, metadatas: CollectionMetadata[]): Observable<number> {
    return defer(() => {
        const pack = tar.pack();
        const output_file = connector.createWriteStream();
        const gzip = zlib.createGzip(connector.gzip);

        const metadatas_pack = [];

        for (const metadata of metadatas) {
            metadatas_pack.push(packMetadata$(pack, metadata));
        }

        // pack streams
        const metadataPack$ = concat(...metadatas_pack).pipe(toArray(), map(() => 0));

        const contentPack$ = data$.pipe(
            groupBy(({ metadata: { name } }) => name, (writable: WritableData) => writable, undefined, () => new ReplaySubject()),
            concatMap((dataByCollectionName$) =>
                dataByCollectionName$.pipe(
                    groupBy(({ metadata: { size } }) => size, ({ batch }: WritableData) => batch, undefined, () => new ReplaySubject()),
                    first(), // taking only the first group by size
                    concatMap((collectionData$) => {
                        return packCollectionData$(pack, { name: dataByCollectionName$.key, size: collectionData$.key }, collectionData$);
                    })
                )
            ),
        );

        // executing the stream
        const file_close_promise = fromEvent(
            pack.pipe(gzip).pipe(output_file),
            'close'
        ).pipe(take(1)).toPromise();

        const finalizing$ = defer(async () => {
            pack.finalize();

            return 0;
        });

        const closing$ = defer(async () => {
            await file_close_promise;

            return 0;
        });

        const packing$ = concat(metadataPack$, contentPack$).pipe(
            catchError((err) => {
                return concat(finalizing$, closing$, throwError(err));
            })
        );


        return concat(packing$, finalizing$, closing$);
    });
}

function packMetadata$(pack: tar.Pack, metadata: CollectionMetadata): Observable<CollectionMetadata> {
    return new Observable((observer: Observer<CollectionMetadata>) => {
        const content = JSON.stringify({
            options: {},
            indexes: metadata.indexes,
            uuid: "",
        });

        pack.entry({ name: `${metadata.name}.metadata.json` }, content, (error) => {
            if (error) {
                observer.error(error);
            } else {
                observer.next(metadata);
                observer.complete();
            }
        });
    });
}

function packCollectionData$(pack: tar.Pack, metadata: { name: string, size: number }, data$: Observable<Buffer>): Observable<number> {
    return new Observable((observer: Observer<number>) => {

        const entry = pack.entry({ name: `${metadata.name}.bson`, size: metadata.size }, (error) => {
            if (error) {
                observer.error(error);
            } else {
                observer.complete();
            }
        });

        const subscription = data$.pipe(
            concatMap(async (data) => {
                return await new Promise<number>((resolve, reject) => {
                    entry.write(data, (error) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(data.length);
                        }
                    })
                })
            }),
        ).subscribe(
            observer.next.bind(observer),
            (error) => {
                entry.end();
            }, () => {
                entry.end();
            });

        return () => {
            subscription.unsubscribe();
        }
    });
}
