import { TargetConnector, SourceConnector, CollectionData } from "../Connector";
import { Readable, Writable } from "stream";
import { GzipOpts, CollectionMetadata } from "../../contracts";
import { Observable, Observer, concat, ReplaySubject, defer, fromEvent, throwError } from "rxjs";
import { groupBy, concatMap, toArray, map, take, catchError } from 'rxjs/operators';
import * as tar from "tar-stream";
import * as zlib from "zlib";
import { Validatable } from "../Validatable";

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

    write(collections: CollectionData[]) {
        return defer(() => {
            const pack = tar.pack({
                highWaterMark: this.astarget.bulk_write_size,
            });

            const output_file = this.createWriteStream();
            const gzip = zlib.createGzip(this.astarget.gzip);

            // pack streams
            const metadata$ = concat(...collections.map(({ metadata }) => packMetadata$(pack, metadata))).pipe(toArray(), map(() => 0));
            const content$ = concat(...collections.map(({ metadata: { name, size }, data$: collectionData$ }) => packCollectionData$(pack, { name, size }, collectionData$)));

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

    data$(collection_name: string): Observable<Buffer> {
        return new Observable((observer: Observer<Buffer>) => {
            const extract = tar.extract({
                highWaterMark: this.assource.bulk_read_size
            });

            const file_input_stream = this.createReadStream();
            const unzip = zlib.createGunzip(this.assource.gzip);

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
            })
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
