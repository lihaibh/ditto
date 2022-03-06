import { TargetConnector, SourceConnector, CollectionData } from "../Connector";
import { Readable, Writable } from "stream";
import { GzipOpts, CollectionMetadata } from "../../contracts";
import { Observable, Observer, concat, ReplaySubject, defer, fromEvent, throwError, of } from "rxjs";
import { groupBy, concatMap, toArray, map, take, catchError, scan, filter, share, takeLast, switchMap } from 'rxjs/operators';
import * as tar from "tar-stream";
import * as zlib from "zlib";
import { Validatable } from "../Validatable";
import { VError } from 'verror';

export interface FileSystemSourceConnector extends SourceConnector {
    createReadStream(): Readable;
}

export interface FileSystemTargetConnector extends TargetConnector {
    createWriteStream(): Writable
}

interface PartialCollectionMetadata {
    name: string;
    size?: number;
    indexes?: any[];
}

export abstract class FileSystemDuplexConnector extends Validatable implements FileSystemSourceConnector, FileSystemTargetConnector {

    write(datas: CollectionData[], metadatas: CollectionMetadata[]) {
        return defer(() => {
            const pack = tar.pack({
                highWaterMark: this.astarget.bulk_write_size,
            });

            const output_file = this.createWriteStream();
            const gzip = zlib.createGzip(this.astarget.gzip);

            // pack streams
            const metadata$ = concat(
                ...metadatas.map((metadata) => packMetadata$(pack, metadata))
            ).pipe(toArray(), map(() => 0));

            const content$ = concat(
                ...datas.map(({ metadata: { name, size }, chunk$: collectionData$ }) =>
                    packCollectionData$(pack, { name, size }, this.astarget.bulk_write_size, collectionData$)
                        .pipe(catchError((err) =>
                            throwError(new VError(err, `pack collection: ${name} failed`))
                        ))
                )

            );

            // executing the stream
            const file_close_promise = fromEvent(
                pack.pipe(gzip).pipe(output_file),
                'close'
            ).pipe(take(1)).toPromise();

            const finalizing$ = defer(async () => {
                pack.finalize();
            });

            const closing$ = defer(async () => {
                await file_close_promise;
            });

            const ending$ = concat(finalizing$, closing$).pipe(toArray(), map(() => 0));

            const packing$ = concat(metadata$, content$).pipe(
                catchError((err) => {
                    return concat(ending$, throwError(err));
                })
            );

            return concat(packing$, ending$);
        });
    }

    chunk$({
        name: collection_name
    }: CollectionMetadata): Observable<Buffer> {
        return new Observable((observer: Observer<Buffer>) => {
            const extract = tar.extract({
                highWaterMark: this.assource.bulk_read_size
            });

            const file_input_stream = this.createReadStream();
            const unzip = zlib.createGunzip(this.assource.gzip);

            extract.on('entry', ({ name }, source_stream, next) => {
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
            const extract = tar.extract({
                highWaterMark: this.assource.bulk_read_size
            });

            const file_input_stream = this.createReadStream();
            const unzip = zlib.createGunzip(this.assource.gzip);

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
                        indexes,
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

    abstract type: string;
    abstract connection: any;
    abstract assource: { [key: string]: any; gzip: GzipOpts; bulk_read_size: number };
    abstract astarget: { [key: string]: any; remove_on_failure: boolean; remove_on_startup: boolean; gzip: GzipOpts; bulk_write_size: number; };

    abstract createReadStream(batch_size?: number): Readable
    abstract createWriteStream(): Writable;

    abstract remove(): Promise<boolean>;

    abstract connect(): Promise<any>;
    abstract close(): Promise<any>;

    abstract exists(): Promise<boolean>;
    abstract fullname(): Promise<string>;
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

function packCollectionData$(pack: tar.Pack, metadata: { name: string, size: number },
    fillEmptyChunkSize: number,
    chunk$: Observable<Buffer>): Observable<number> {

    return new Observable((observer: Observer<number>) => {
        let streamError: Error | null = null;

        /* 
        * the data might be bigger or smaller than the metadata size by this point.
        * This is because collections can be altered during the packing process (insert / remove / modifying documents).
        * so in order to perfectly fill the collection backup file with the right amount of data,
        * it is necessary to create a stream of empty chunks if the actual data stream is smaller than the metadata size
        * and trimming the stream of data if it is bigger than the metadata size.
        */
        // when the data stream is bigger than the metadata size of the collection
        const trimmed$ = getTrimmedChunk$({
            chunk$,
            totalBytes: metadata.size
        }).pipe(
            share()
        );

        const content$ = trimmed$.pipe(map(({ chunk }) => chunk)) as Observable<Buffer>;

        // handling when the data stream is smaller than the metadata size of the collection
        const remain$ = concat(
                trimmed$.pipe(takeLast(1), map(({accumulatedTotalBytes: totalBytes}) => metadata.size - totalBytes)),
                // this is necessary to handle a case when the data stream is empty
                of(metadata.size),
        ).pipe(
            // always emits one value, either the remaining bytes by the actual size of the chunk stream or metadata size
            take(1),
            switchMap((remainBytes) =>
                getEmptyChunk$({
                    chunkSize: fillEmptyChunkSize,
                    totalBytes: remainBytes
                })
            )
        );

        const write$ = concat(content$, remain$);

        const entry = pack.entry({ name: `${metadata.name}.bson`, size: metadata.size }, (error) => {
            if (streamError) {
                observer.error(streamError);
            } else if (error) {
                observer.error(error);
            } else {
                observer.complete();
            }
        });

        const subscription = write$.pipe(
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
            })
        ).subscribe(
            (chunk) => {
                observer.next(chunk);
            },
            (error) => {
                streamError = error;

                entry.end();
            }, () => {
                entry.end();
            });

        return () => {
            subscription.unsubscribe();
        }
    });
}

function getTrimmedChunk$(opts: {
    chunk$: Observable<Buffer>,
    totalBytes: number,
}) {
    return opts.chunk$.pipe(
        // cutting extra data from the stream
        scan<Buffer, { accumulatedTotalBytes: number, chunk?: Buffer }>(
            (acc, chunk) => {
                const remainingBytes = opts.totalBytes - acc.accumulatedTotalBytes;

                if (remainingBytes > 0) {
                    if (chunk.length < remainingBytes) {
                        return {
                            chunk,
                            accumulatedTotalBytes: acc.accumulatedTotalBytes + chunk.length
                        }
                    } else {
                        return {
                            chunk: chunk.slice(0, remainingBytes),
                            accumulatedTotalBytes: opts.totalBytes
                        }
                    }
                } else {
                    return {
                        accumulatedTotalBytes: opts.totalBytes
                    }
                }

            },
            {
                accumulatedTotalBytes: 0,
            }
        ),
        filter(({
            accumulatedTotalBytes: totalBytes,
            chunk
        }) => {
            return totalBytes > 0 && chunk !== undefined
        })
    )
}

function getEmptyChunk$(opts: {
    totalBytes: number,
    chunkSize: number,
}) {
    // check remaining bytes
    let remainingBytes = opts.totalBytes;

    return new Observable<Buffer>((observer) => {
        const totalChunks = Math.floor(opts.totalBytes / opts.chunkSize);

        for (let i = 0; i < totalChunks; i++) {
            observer.next(Buffer.alloc(opts.chunkSize));

            remainingBytes -= opts.chunkSize;
        }

        if (remainingBytes > 0) {
            observer.next(Buffer.alloc(remainingBytes));

            remainingBytes = 0;
        }

        observer.complete();
    });
}
