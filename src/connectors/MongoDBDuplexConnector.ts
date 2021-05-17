
import { MongoClient, Db, Collection, Cursor } from "mongodb";
import { Observable, defer, EMPTY, from, concat, merge } from "rxjs";
import { mergeAll, map, mergeMap, bufferCount, toArray } from 'rxjs/operators';
import * as joi from 'joi';
import * as BSON from 'bson';

import { TargetConnector, SourceConnector, CollectionData } from "./Connector";
import { MongoDBConnection, CollectionMetadata as CollectionMetadata } from "../contracts";
import { Validatable } from "./Validatable";
import { pick, merge as loMerge } from "lodash";
import { eachValueFrom } from "rxjs-for-await";
import { convertAsyncGeneratorToObservable } from "../utils";

const BSON_DOC_HEADER_SIZE = 4;

const schema = joi.object({
    connection: joi.object({
        uri: joi.string().required(),
        dbname: joi.string().required(),
        connectTimeoutMS: joi.number().optional()
    }).required(),
    assource: joi.object({
        bulk_read_size: joi.number().optional(),
        collections: joi.array().items(joi.string()).optional()
    }).required(),
    astarget: joi.object({
        remove_on_failure: joi.boolean().optional(),
        remove_on_startup: joi.boolean().optional(),
        collections: joi.array().items(joi.string()).optional(),
        metadatas: joi.array().items(joi.string()).optional(),
        documents_bulk_write_count: joi.number().optional(),
    }).required(),
});

export interface MongoDBConnectorOptions {
    connection: MongoDBConnection;

    /**
     * data related to this connector as a source
     */
    assource?: Partial<AsSourceOptions>;

    /**
     * data related to this connector as a target
     */
    astarget?: Partial<AsTargetOptions>;
}

export class MongoDBDuplexConnector extends Validatable implements SourceConnector, TargetConnector {
    type = 'mongodb';

    // options
    connection: MongoDBConnection;
    assource: AsSourceOptions;
    astarget: AsTargetOptions;

    // session data (after successful connection)
    private client?: MongoClient;
    private db?: Db;
    private collections?: Collection<any>[];

    constructor({ connection, assource = {}, astarget = {} }: MongoDBConnectorOptions) {
        super();

        this.connection = connection;

        this.assource = loMerge({ bulk_read_size: 50 * 1024 }, assource);

        this.astarget = loMerge({ remove_on_failure: true, remove_on_startup: true, documents_bulk_write_count: 1000 }, astarget);
    }

    // as source
    data$(collection_name: string) {
        return defer(async () => {
            if (!this.db || !this.collections) {
                return EMPTY;
            }

            // check if collection exist
            if (!this.collections.find(collection => collection.collectionName === collection_name)) {
                return EMPTY;
            }

            const collection = this.db.collection(collection_name);
            const data_cursor = collection.find<Buffer>({}, { batchSize: this.assource.bulk_read_size }).stream();

            return cursorToObservalbe(data_cursor);
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
        return `type: ${this.type}, database: ${this.connection.dbname}`;
    }

    options() {
        return pick(this, 'connection', 'assource', 'astarget');
    }

    schema() {
        return schema;
    }

    // as target
    async remove() {
        if (!this.db || !this.client) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        try {
            await this.db.dropDatabase();

            return true;
        } catch (e) {
            return false;
        }
    }

    async writeCollectionMetadata(metadata: CollectionMetadata) {
        if (!this.db || !this.client?.isConnected()) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        // create indexes
        if (metadata.indexes.length > 0) {
            return this.db.collection(metadata.name).createIndexes(metadata.indexes);
        }

        return Promise.resolve();
    }

    writeCollectionData(collection_name: string, data$: Observable<Buffer>): Observable<number> {
        const documents$ = convertAsyncGeneratorToObservable(getDocumentsGenerator(data$));

        return documents$.pipe(
            bufferCount(this.astarget.documents_bulk_write_count),
            mergeMap(async (documents: CollectionDocument[]) => {
                if (documents.length > 0) {
                    await this.insertCollectionDocuments(collection_name, documents as [CollectionDocument]);
                }


                return documents.reduce((size, { raw }) => size + raw.length, 0);
            })
        );
    }

    async insertCollectionDocuments(collection_name: string, documents: [CollectionDocument]) {
        if (!this.db) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        const collection = this.db.collection(collection_name);

        async function insert(documents: [CollectionDocument]) {
            return await collection.bulkWrite(documents.map(({ obj: document }) => ({
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

        return await insert(documents)
            .catch(async function () {
                documents.forEach(({ obj: document }) => filterInvalidKeys(document, key => key && key.includes('.')));

                return await insert(documents);
            })
    }

    write(datas: CollectionData[], metadatas: CollectionMetadata[]): Observable<number> {
        return defer(() => {
            const metadata$ = merge(
                ...metadatas.map((metadata) =>
                    from(this.writeCollectionMetadata(metadata))
                )).pipe(toArray(), map(() => 0));

            const content$ = merge(...datas.map(({ metadata, data$ }) =>
                this.writeCollectionData(metadata.name, data$))
            );

            return concat(metadata$, content$);
        });
    }

    async connect() {
        const client = new MongoClient(this.connection.uri, {
            connectTimeoutMS: this.connection.connectTimeoutMS || 5000,
            raw: true,
            keepAlive: true,
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });

        this.client = await client.connect();

        this.db = this.client.db(this.connection.dbname);
        this.collections = await this.db.collections();
    }

    async close() {
        if (this.client?.isConnected()) {
            await this.client.close();
        }

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

async function* getDocumentsGenerator(data$: Observable<Buffer>): AsyncGenerator<CollectionDocument> {
    let buffer = Buffer.alloc(0);

    for await (const data of eachValueFrom(data$)) {
        buffer = Buffer.concat([buffer, data]);

        let next_doclen = null;

        if (buffer.length >= BSON_DOC_HEADER_SIZE) {
            next_doclen = buffer.readInt32LE(0);
        } else {
            next_doclen = null;
        }

        // flush all documents from the buffer
        while (next_doclen && buffer.length >= next_doclen) {
            const raw = buffer.slice(0, next_doclen);
            const obj = BSON.deserialize(raw);

            buffer = buffer.slice(next_doclen);

            yield {
                raw,
                obj
            };

            if (buffer.length >= BSON_DOC_HEADER_SIZE) {
                next_doclen = buffer.readInt32LE(0);
            } else {
                next_doclen = null;
            }
        }
    }
}

function cursorToObservalbe<T>(cursor: Cursor<T>): Observable<T> {
    return new Observable((observer) => {
        cursor.forEach(
            observer.next.bind(observer),
            (err) => {
                if (err) {
                    observer.error(err);
                } else {
                    observer.complete();
                }
            });
    })
}

interface CollectionDocument {
    raw: Buffer, obj: { [key: string]: any }
}

interface AsSourceOptions {
    /**
     * The amount of bytes to read (in bson format) from mongodb database each time.
     * The bigger the number is it will improve the performance as it will decrease the amount of reads from the database (less io consumption).
     */
    bulk_read_size: number;

    /**
    * collections to read from the mongodb database.
    * If its empty, the filter is skipped, reading all the collections from the database.
    */
    collections?: string[];
}

interface AsTargetOptions {
    /**
    * Whether or not to remove the target object in case of an error.
    */
    remove_on_failure: boolean;

    /**
     * Whether or not to remove the target object if its exist before transfering content to it.
     * It can help avoiding conflicts when trying to write data that already exist on the target connector.
     */
    remove_on_startup: boolean;

    /**
    * collections to write into the mongodb database.
    * If its empty, the filter is skipped, writing all the collections from the source.
    */
    collections?: string[];

    /**
    * metadata of collections to write into mongodb.
    * If its empty, the filter is skipped, writing metadata of all the collections.
    */
    metadatas?: string[];

    /**
    * The amount of documents to write in each operation.
    * The greater this number is the better performance it will provide as it will make less writes to the MongoDB server.
    */
    documents_bulk_write_count: number;
}