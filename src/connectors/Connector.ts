import { CollectionMetadata } from "../contracts";
import { Observable } from "rxjs";

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

export interface SourceConnector extends Connector {
    /**
     * data related to this connector as a source
     */
    assource: {
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
     * @param collection_name the collection name from the source object
     */
    data$(collection_name: string): Observable<Buffer>;
}

export interface TargetConnector extends Connector {
    /**
     * data related to this connector as a target
     */
    astarget: {
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
        * collections to write into the target connector.
        * If its empty, the filter is skipped, writing all the collections from the source.
        */
        collections?: string[];

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
     * @return a stream of amount of data written to the data source object.
     */
    write(collections: CollectionData[]): Observable<number>;
}

export interface CollectionData {
    data$: Observable<Buffer>;
    metadata: CollectionMetadata;
}
