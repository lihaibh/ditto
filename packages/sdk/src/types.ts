export interface CollectionDocument {
    // in bson format
    raw: Buffer;
    // the deserialised object
    obj: { [key: string]: any }
}
