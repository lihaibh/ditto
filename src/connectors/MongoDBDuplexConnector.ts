import { TargetConnector, SourceConnector, WritableData } from "./Connector";
import { MongoDBConnection, CollectionMetadata as CollectionMetadata } from "../contracts";
import { MongoClient, Db, Collection, Cursor } from "mongodb";
import { Observable, defer, EMPTY, ReplaySubject, interval, Observer } from "rxjs";
import { mergeAll, switchMap, tap, map, groupBy, mergeMap, bufferCount, concatMap } from 'rxjs/operators';
import { streamToRx } from 'rxjs-stream';
import * as joi from 'joi';
import { Validatable } from "./Validatable";
import { pick } from "lodash";
import { eachValueFrom } from "rxjs-for-await";

import * as BSON from 'bson';

const BSON_DOC_HEADER_SIZE = 4;

const schema = joi.object({
    connection: joi.object({
        uri: joi.string().required(),
        dbname: joi.string().required(),
        connectTimeoutMS: joi.number().optional()
    }).required(),
    remove_on_failure: joi.boolean().optional(),
    bulkWriteSize: joi.number().optional(),
});

export interface MongoDBConnectorOptions {
    connection: MongoDBConnection;
    remove_on_failure?: boolean;
    remove_on_startup?: boolean;

    /**
     * The amount of documents to write in each operation.
     * The greater this number is the better performance it will provide as it will make less writes to the MongoDB server.
     */
    bulkWriteSize?: number;
}

interface CollectionDocument {
    raw: Buffer, obj: { [key: string]: any }
}

export class MongoDBDuplexConnector extends Validatable implements SourceConnector, TargetConnector {
    name = 'mongodb';
    connection: MongoDBConnection;
    remove_on_failure: boolean;
    remove_on_startup: boolean;
    bulkWriteSize: number;

    private client?: MongoClient;
    private db?: Db;
    private collections?: Collection<any>[];

    constructor({ connection, remove_on_failure = false, remove_on_startup = false, bulkWriteSize = 5000 }: MongoDBConnectorOptions) {
        super();

        this.connection = connection;
        this.remove_on_failure = remove_on_failure;
        this.remove_on_startup = remove_on_startup;
        this.bulkWriteSize = bulkWriteSize;
    }

    // as source
    batch$(collection_name: string, batch_size: number) {
        return defer(async () => {
            if (!this.db) {
                return EMPTY;
            }

            const collection = this.db.collection(collection_name);
            const data_stream = collection.find().batchSize(batch_size) as NodeJS.ReadableStream;

            return streamToRx(data_stream) as Observable<Buffer>
        }).pipe(
            mergeAll()
        )
    }

    async exists() {
        if (!this.db || !this.client) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        return this.client.isConnected() && this.db.databaseName === this.connection.dbname;
    }

    async fullname() {
        return this.name;
    }

    options() {
        return pick(this, 'connection', 'remove_on_failure', 'bulkWriteSize');
    }

    schema() {
        return schema;
    }

    // as target
    remove(): Promise<boolean> {
        if (!this.db || !this.client) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        return this.db.dropDatabase().then(() => true).catch(() => false);
    }

    async writeCollectionMetadata(metadata: CollectionMetadata) {
        if (!this.db || !this.client?.isConnected()) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        return this.db.collection(metadata.name).createIndexes(metadata.indexes)
    }

    writeCollectionData(collection_name: string, data$: Observable<Buffer>): Observable<number> {
        return Observable.create(async (observer: Observer<CollectionDocument>) => {
            let buffer = Buffer.alloc(0);
            let next_doclen = null;

            for await (const data of eachValueFrom(data$)) {
                buffer = Buffer.concat([buffer, data]);

                // flush all documents from the buffer
                let next_doclen = null;

                if (buffer.length >= BSON_DOC_HEADER_SIZE) {
                    next_doclen = buffer.readInt32LE(0);
                } else {
                    next_doclen = null;
                }

                while (next_doclen && buffer.length >= next_doclen) {
                    const raw = buffer.slice(0, next_doclen);
                    const obj = BSON.deserialize(raw);

                    buffer = buffer.slice(next_doclen);

                    observer.next({
                        raw,
                        obj
                    });

                    if (buffer.length >= BSON_DOC_HEADER_SIZE) {
                        next_doclen = buffer.readInt32LE(0);
                    } else {
                        next_doclen = null;
                    }
                }
            }

            observer.complete();

        }).pipe(
            bufferCount(this.bulkWriteSize),
            mergeMap(async (documents: [CollectionDocument]) => {
                await this.insertCollectionDocuments(collection_name, documents);

                return documents.reduce((size, { raw }) => size + raw.length, 0);
            })
        );
    }

    async insertCollectionDocuments(collection_name: string, documents: [CollectionDocument]) {
        if (!this.db) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        const collection = this.db.collection(collection_name);

        function insert(documents: [CollectionDocument]) {
            return collection.bulkWrite(documents.map(({ obj: document }) => ({
                insertOne: {
                    document
                }
            }),
                {
                    ordered: false,
                    bypassDocumentValidation: true,
                },
            ))
        }

        insert(documents)
            .catch(async function () {
                documents.forEach(({ obj: document }) => filterInvalidKeys(document, key => key && key.includes('.')));

                return await insert(documents);
            })
    }

    write(data$: Observable<WritableData>, metadatas: CollectionMetadata[]): Observable<number> {
        // write the metadata first
        return defer(async () => {
            const push_metadata_promises = metadatas.map(async (metadata) =>
                this.writeCollectionMetadata(metadata)
            );

            return await Promise.all(push_metadata_promises);
        }).pipe(
            switchMap(() => data$.pipe(
                groupBy(({ metadata: { name } }) => name, (writable: WritableData) => writable, undefined, () => new ReplaySubject()),
                concatMap((group$) => this.writeCollectionData(group$.key, group$.pipe(map(writable => writable.batch))))
            )),
        );
    }

    async connect() {
        const client = new MongoClient(this.connection.uri, {
            connectTimeoutMS: this.connection.connectTimeoutMS,
            raw: true,
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });

        this.client = await client.connect();

        this.db = this.client.db(this.connection.dbname);
        this.collections = await this.db.collections();
    }

    async close() {
        await this.client?.isConnected() && this.client?.close();

        this.client = undefined;
        this.db = undefined;
        this.collections = undefined;
    }

    async transferable() {
        if (!this.client?.isConnected() || !this.collections) {
            throw new Error(`MongoDB client is not connected`);
        }

        const all_collections = this.collections.filter(
            (collection) => !collection.collectionName.startsWith("system.")
        );

        return (await Promise.all(all_collections.map(async (collection) => {
            try {
                const [stats, indexes] = await Promise.all([collection.stats(), collection.indexes()]);

                return {
                    name: collection.collectionName,
                    size: stats.size,
                    indexes,
                };
            }
            catch (e) {
                console.warn(`ignoring collection: "${collection.collectionName}", as we couldnt receive details due to an error: ${e.message}`);

                return null;
            }
        }))).filter(notEmpty);
    }

    /**
     * Get a stream of specific collection׳s documents from the database.
     * 
     * @param collection_name the name of the collection to fetch
     * @param batchSize how many documents to fetch on each iteration
     */
    stream(collection_name: string, batchSize: number) {
        if (!this.client?.isConnected() || !this.collections) {
            throw new Error(`MongoDB client is not connected`);
        }

        return this.collections.find(collection => collection.collectionName === collection_name)?.find().batchSize(batchSize);
    }
}

function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
}

function filterInvalidKeys(obj: { [key: string]: any }, filterKeyFn: (key: string) => any) {
    const invalid_keys = Object.keys(obj).filter(key => filterKeyFn(key));

    for (const invalid_key of invalid_keys) {
        delete obj[invalid_key];
    }

    for (let k in obj) {
        if (obj[k] && typeof obj[k] === 'object') {
            filterInvalidKeys(obj[k], filterKeyFn);
        }
    }
}
