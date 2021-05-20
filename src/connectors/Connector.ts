import { CollectionMetadata } from "../contracts";
import { Observable } from "rxjs";
import * as joi from 'joi';

export type Schema<T> = Required<{
    [K in keyof T]: joi.AnySchema;
}>;

/**
 * Represent an interface to access specific object that contains collections in the data source.
 */
interface Connector {
    type: string;
    connection: any;

    /**
     * connecting to a certain data source using the connection details.
     */
    connect(): Promise<any>;

    /**
     * Disconneting from the data source.
     */
    close(): Promise<any>;

    /**
     * validates the connection details to the object.
     */
    validate(): void;

    /**
     * check if object exist in data source.
     */
    exists(): Promise<boolean>;

    /**
     * get object׳s full name
     */
    fullname(): Promise<string>;
};

export interface SourceConnectorBaseOptions {
    /**
     * The amount of bytes to read (in bson format) from the data source object each time.
     * The bigger the number is it will improve the performance as it will decrease the amount of reads from the data source object (less io consumption).
     */
    bulk_read_size: number;

    /**
     * collections to read from the source connector.
     * If its empty, the filter is skipped, reading all the collections from the source.
     */
    collections?: string[];

    /**
     * collection to exclude from reading from the source connector.
     * If its empty the filter is ignored.
     * if both collections and exclude collections provided, the exclude will have preferability over the include collections.
     * You can provide the literal name of the collection or a regex.
     */
    exclude_collections?: string[];
}

export const SOURCE_CONNECTOR_BASE_OPTIONS_SCHEMA: Schema<SourceConnectorBaseOptions> = {
    bulk_read_size: joi.number().optional(),
    collections: joi.array().items(joi.string()).optional(),
    exclude_collections: joi.array().items(joi.string()).optional(),
};

export interface SourceConnector extends Connector {
    /**
     * data related to this connector as a source
     */
    assource: SourceConnectorBaseOptions & {
        // aditional properties
        [key: string]: any;
    }

    /**
     * get all collections metadata from the source that can be transferred
     */
    transferable(): Promise<CollectionMetadata[]>;

    /**
     * get stream of documents from the object inside the data source
     * 
     * @param metadata the collection׳s metadata from the source object
     */
    chunk$(metadata: CollectionMetadata): Observable<Buffer>;
}

export interface TargetConnectorBaseOptions {
    /**
    * Whether or not to remove the target object in case of an error.
    */
    remove_on_failure: boolean;

    /**
     * Whether or not to remove the target object if it exist before transfering content to it.
     * It can help avoiding conflicts when trying to write data that already exist on the target connector.
     */
    remove_on_startup: boolean;

    /**
     * collections to write into the target connector.
     * If its empty, the filter is ignored.
     * You can provide the literal name of the collection or a regex.
     */
    collections?: string[];

    /**
     * collection to exclude from writing to the target connector.
     * If its empty the filter is ignored.
     * if both collections and exclude collections provided, the exclude will have preferability over the include collections.
     * You can provide the literal name of the collection or a regex.
     */
    exclude_collections?: string[];

    /**
     * metadata of collections to write into the target connector.
     * If its empty, the filter is skipped, writing metadata of all the collections.
     */
    metadatas?: string[];

    /**
     * metadatas of collections to exclude from writing into the target connector.
     * You can provide the literal name of the collection or a regex. 
     */
    exclude_metadatas?: string[];
}

export const TARGET_CONNECTOR_BASE_OPTIONS_SCHEMA: Schema<TargetConnectorBaseOptions> = {
    remove_on_failure: joi.boolean().optional(),
    remove_on_startup: joi.boolean().optional(),
    collections: joi.array().items(joi.string()).optional(),
    exclude_collections: joi.array().items(joi.string()).optional(),
    metadatas: joi.array().items(joi.string()).optional(),
    exclude_metadatas: joi.array().items(joi.string()).optional()
};

export interface TargetConnector extends Connector {
    /**
     * data related to this connector as a target
     */
    astarget: TargetConnectorBaseOptions & {
        // additional properties
        [key: string]: any;
    }

    /**
     * removing the object that contains the collections
     */
    remove(): Promise<boolean>;

    /**
    * write the collections data and metadata into the data source object.
    * emmits the amount of data written to the data source object.
    * 
    * @param datas the collection data to write into the target connector.
    * @param metadatas the metadata to write into the target connector.
    * 
    * @return a stream of amount of data written to the data source object
    */
    write(datas: CollectionData[], metadatas: CollectionMetadata[]): Observable<number>;
}

export interface CollectionData {
    chunk$: Observable<Buffer>;
    metadata: CollectionMetadata;
}
