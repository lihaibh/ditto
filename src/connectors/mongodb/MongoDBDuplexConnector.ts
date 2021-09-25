
import { MongoClient, Db, Collection, Cursor, FilterQuery } from "mongodb";
import { Observable, defer, EMPTY, from, concat, merge } from "rxjs";
import { mergeAll, map, mergeMap, bufferCount, toArray, filter } from 'rxjs/operators';
import * as joi from 'joi';
import * as BSON from 'bson';

import { TargetConnector, SourceConnector, CollectionData, Schema, SOURCE_CONNECTOR_BASE_OPTIONS_SCHEMA, TARGET_CONNECTOR_BASE_OPTIONS_SCHEMA, SourceConnectorBaseOptions, TargetConnectorBaseOptions } from '../Connector';
import { MongoDBConnection, CollectionMetadata as CollectionMetadata } from "../../contracts";
import { Validatable } from "../Validatable";
import { pick, merge as loMerge, isString, isArray, isFunction, pickBy } from "lodash";
import { eachValueFrom } from "rxjs-for-await";
import { convertAsyncGeneratorToObservable, hasRegexMatch } from '../../utils';

const BSON_DOC_HEADER_SIZE = 4;

const documentFilterSchema = joi.alternatives().try([
    joi.string(),
    joi.func()
]).required();

const schema = joi.object(<Schema<MongoDBConnectorOptions>>{
    connection: joi.object(<Schema<MongoDBConnection>>{
        uri: joi.string().required(),
        dbname: joi.string().required(),
        connectTimeoutMS: joi.number().optional(),
        isAtlasFreeTier: joi.boolean().optional()
    }).required(),
    assource: joi.object(<Schema<AsSourceMongoDBOptions>>{
        ...SOURCE_CONNECTOR_BASE_OPTIONS_SCHEMA,
    }).required(),
    astarget: joi.object(<Schema<AsTargetMongoDBOptions>>{
        ...TARGET_CONNECTOR_BASE_OPTIONS_SCHEMA,
        documents_bulk_write_count: joi.number().optional(),
        upsert: joi.object().pattern(
            joi.string(),
            joi.alternatives(
                documentFilterSchema,
                joi.array().items(documentFilterSchema)
            )
        ).optional(),
        writeDocument: joi.func().optional()
    }).required(),
});

export interface MongoDBConnectorOptions {
    connection: MongoDBConnection;

    /**
     * data related to this connector as a source
     */
    assource?: Partial<AsSourceMongoDBOptions>;

    /**
     * data related to this connector as a target
     */
    astarget?: Partial<AsTargetMongoDBOptions>;
}

type AsSourceMongoDBOptions = SourceConnectorBaseOptions;
type AsTargetMongoDBOptions = TargetConnectorBaseOptions & {
    /**
    * The amount of documents to write in each operation.
    * The greater this number is the better performance it will provide as it will make less writes to the MongoDB server.
    */
    documents_bulk_write_count: number;

    /**
    * insert or update exist documents on the target connector, overriding them by searching specific fields.
    * the upsert options is a dictionary representation of a collection selector to a document filter.
    * The collection selector can either be a regular expression or the literal name of the collection.
    * The document filter can be the literal name or regex of the field / fields or a function to perform the search of an exist document by (it is recommended to use something that compose a unique key).
    * @example
    * ```
    * {
    *   ...
    *   upsert: {
    *       "prefix_.*_suffix": (document) => pick(document, ['p1', 'p2', 'p3']),
    *       "collection_0": (document) => ({ p1: 'p1', p2: { $gte: 0 }}),
    *       "collection_1": ['p1', 'p2', 'p3'],
    *       "collection_2": ['p.*'],
    *       ".*": "_id"
    *   }
    * }
    * ```
    */
    upsert?: {
        [collection: string]: DocumentFilter | DocumentFilter[];
    };

    /**
     * allows you to filter specific documents to write into the target connector.
     * if this function returns true, the document will be written to the target connector.
     */
    writeDocument?: (collection: string, document: any) => boolean;
}

type DocumentFilterFunction<D = any> = (doc: D) => FilterQuery<D>;
type DocumentFilter = string | DocumentFilterFunction;

interface CollectionDocument {
    raw: Buffer, obj: { [key: string]: any }
}
export class MongoDBDuplexConnector extends Validatable implements SourceConnector, TargetConnector {
    type = 'MongoDB Connector';

    // options
    assource: AsSourceMongoDBOptions;
    astarget: AsTargetMongoDBOptions;

    connection: MongoDBConnection;

    // session data (after successful connection)
    private client?: MongoClient;
    private db?: Db;
    private collections?: Collection<any>[];

    constructor({ connection, assource = {}, astarget = {} }: MongoDBConnectorOptions) {
        super();

        this.connection = connection;

        this.assource = loMerge({ bulk_read_size: 50 * 1024 }, assource);
        this.astarget = loMerge({ remove_on_failure: false, remove_on_startup: false, documents_bulk_write_count: 1000 }, astarget);
    }

    // as source
    chunk$({
        name: collection_name,
    }: CollectionMetadata) {
        return defer(async () => {
            if (!this.db || !this.collections) {
                return EMPTY;
            }

            // check if collection exist
            if (!this.collections.find(collection => collection.collectionName === collection_name)) {
                return EMPTY;
            }

            const adminDb = this.db.admin();

            const { version } = await adminDb.serverStatus();

            let majorVersion = version.split('.')[0];

            majorVersion = majorVersion && Number(majorVersion);

            const collection = this.db.collection(collection_name);

            // removes timeout property if in atlas free tier, since noTimeout cursors are forbidden (the property timeout cannot be set to any value)
            let chunkCursor = collection.find<Buffer>({}, { timeout: this.connection.isAtlasFreeTier ? undefined : false, batchSize: this.assource.bulk_read_size });
            
            if (majorVersion < 4) {
                chunkCursor = chunkCursor.snapshot(true as any);
            }

            return cursorToObservalbe(chunkCursor);
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
            // deletes v property from indexes metadata if in atlas free tier
            this.connection.isAtlasFreeTier && metadata.indexes.forEach((i) => delete i.v)
            return this.db.collection(metadata.name).createIndexes(metadata.indexes);
        }

        return Promise.resolve();
    }

    writeCollectionData(collection_name: string, chunk$: Observable<Buffer>): Observable<number> {
        const documents$ = convertAsyncGeneratorToObservable(getDocumentsGenerator(chunk$))
            .pipe(filter(({ obj: document }) => {
                // filter documents to write into mongodb
                return this.astarget.writeDocument ? this.astarget.writeDocument(collection_name, document) : true
            }));

        return documents$.pipe(
            bufferCount(this.astarget.documents_bulk_write_count),
            mergeMap(async (documents: CollectionDocument[]) => {
                if (documents.length > 0) {
                    await this.writeCollectionDocuments(collection_name, documents as [CollectionDocument]);
                }


                return documents.reduce((size, { raw }) => size + raw.length, 0);
            })
        );
    }

    async writeCollectionDocuments(collectionName: string, documents: [CollectionDocument]) {
        if (!this.db) {
            throw new Error("Need to connect to the data source before using this method.");
        }

        const collection = this.db.collection(collectionName);
        const upsert = this.astarget.upsert;

        const replaceFilter = Object.entries(upsert || {}).reduce((docFilterFn: (DocumentFilterFunction | undefined), [collectionSelector, collectionDocumentFilters]) => {
            if (hasRegexMatch(collectionSelector, collectionName)) {
                collectionDocumentFilters = (!isArray(collectionDocumentFilters)) ? [collectionDocumentFilters] : collectionDocumentFilters;

                const currCollectionDocumentFilterFn = collectionDocumentFilters.reduce<DocumentFilterFunction | undefined>((collectionDocumentFilterFn, filter) => {
                    let documentFilterFn: DocumentFilterFunction | undefined = undefined;

                    if (isFunction(filter)) {
                        documentFilterFn = filter;
                    } else if (isString(filter)) {
                        documentFilterFn = (document: object) => {
                            return pickBy(document, (value, property) => {
                                return hasRegexMatch(filter, property);
                            }) as DocumentFilterFunction;
                        };
                    }

                    if (documentFilterFn) {
                        return (document) => {
                            if (collectionDocumentFilterFn) {
                                // merge results of filters
                                return <FilterQuery<any>>{
                                    $and: [
                                        collectionDocumentFilterFn(document),
                                        (documentFilterFn as DocumentFilterFunction)(document)
                                    ]
                                }
                            }

                            return (documentFilterFn as DocumentFilterFunction)(document);
                        }
                    }

                    return collectionDocumentFilterFn;
                }, undefined);

                if (currCollectionDocumentFilterFn) {
                    return (document) => {
                        if (docFilterFn) {
                            return <FilterQuery<any>>{
                                $or: [
                                    docFilterFn(document),
                                    (currCollectionDocumentFilterFn as DocumentFilterFunction)(document)
                                ]
                            }
                        }

                        return (currCollectionDocumentFilterFn as DocumentFilterFunction)(document);
                    }
                }
            }

            return docFilterFn;
        }, undefined);

        async function write(documents: [CollectionDocument]) {
            return await collection.bulkWrite(documents.map(({ obj: document }) => ({

                // either replacing or inserting new document for each income document 
                ...(replaceFilter ? ({
                    replaceOne: {
                        upsert: true,
                        replacement: document,
                        filter: replaceFilter(document),
                    }
                }) : ({
                    insertOne: {
                        document
                    }
                })),

            }),
                {
                    ordered: false,
                    bypassDocumentValidation: true,
                },
            ))
        }

        return write(documents)
            .catch(async function () {
                // there sometimes failures during insertions because documents contain properties with invalid key names (containing the ".")
                documents.forEach(({ obj: document }) => filterInvalidKeys(document, key => key && key.includes('.')));

                return write(documents);
            })
    }

    write(datas: CollectionData[], metadatas: CollectionMetadata[]): Observable<number> {
        return defer(() => {
            const metadata$ = merge(
                ...metadatas.map((metadata) =>
                    from(this.writeCollectionMetadata(metadata))
                )).pipe(toArray(), map(() => 0));

            const content$ = merge(...datas.map(({ metadata, chunk$ }) =>
                this.writeCollectionData(metadata.name, chunk$))
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
                    count: stats.count,
                    indexes,
                };
            }
            catch (e) {
                console.warn(`ignoring collection: "${collection.collectionName}", as we couldnt receive details due to an error: ${(e as any).message}`);

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

async function* getDocumentsGenerator(chunk$: Observable<Buffer>): AsyncGenerator<CollectionDocument> {
    let buffer = Buffer.alloc(0);

    for await (const data of eachValueFrom(chunk$)) {
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
