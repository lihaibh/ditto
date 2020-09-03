import { CollectionMetadata } from "../contracts";
import { Observable } from "rxjs";

/**
 * Represent an interface to access specific object that contains collections in the data source.
 */
interface Connector {
    name: string;
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

export interface SourceConnector extends Connector {
    /**
     * get all collections metadata from the source that can be transferred
     */
    transferable(): Promise<CollectionMetadata[]>;

    /**
     * get stream of documents from the object inside the data source
     * 
     * @param collection_name the collection name from the source object
     * @param batch_size what will be the size of the batch (the bson amount of bytes to read each time).
     */
    batch$(collection_name: string, batch_size: number): Observable<Buffer>;
}

export interface TargetConnector extends Connector {
    /**
     * Whether or not to remove the target object in case of an error
     */
    remove_on_failure: boolean;

    /**
     * Whether or not to remove the target object when transfering content to it.
     * It can help avoiding conflicts when trying to write data that already exist on the target connector.
     */
    remove_on_startup: boolean;

    /**
     * removing the object that contains the collections
     */
    remove(): Promise<boolean>;

    /**
     * write the collections data and metadata into the data source object.
     * emmits the amount of data written to the data source object.
     * 
     * @return a stream of amount of data written to the data source object.
     */
    write(source: Observable<WritableData>, metadata: CollectionMetadata[]): Observable<number>;
}

export interface DocumentBatch {
    bytes: number;
    documents: any[];
}

export interface CollectionToWrite {
    batch$: Observable<DocumentBatch>;
    metadata: CollectionMetadata;
}

export interface WritableData {
    batch: Buffer; // in bson format
    metadata: CollectionMetadata;
}