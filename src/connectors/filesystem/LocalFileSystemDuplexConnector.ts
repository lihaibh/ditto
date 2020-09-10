import { createReadStream, createWriteStream, access, unlink } from "fs";
import * as joi from "joi";
import { pick, merge } from "lodash";
import { promisify } from 'util';
import * as zlib from 'zlib';

import { FileSystemDuplexConnector } from "./FileSystemDuplexConnector";
import { GzipOpts } from "../../contracts";

const accessP = promisify(access);
const unlinkP = promisify(unlink);

const gzipSchema = joi.object({
    flush: joi.number().optional(),
    finishFlush: joi.number().optional(),
    chunkSize: joi.number().optional(),
    windowBits: joi.number().optional(),
    level: joi.number().optional(),
    memLevel: joi.number().optional(),
    strategy: joi.number().optional(),
}).required();

const schema = joi.object({
    connection: joi.object({
        path: joi.string().required()
    }).required(),
    assource: joi.object({
        bulk_read_size: joi.number().optional(),
        collections: joi.array().items(joi.string()).optional(),
        gzip: gzipSchema
    }).required(),
    astarget: joi.object({
        remove_on_failure: joi.boolean().optional(),
        remove_on_startup: joi.boolean().optional(),
        collections: joi.array().items(joi.string()).optional(),
        gzip: gzipSchema,
        bulk_write_size: joi.number().optional()
    }).required(),

});

export interface LocalFileSystemOptions {
    connection: LocalFileSystemConnection;

    /**
     * data related to this connector as a source
     */
    assource?: Partial<AsSourceOptions>;

    /**
     * data related to this connector as a target
     */
    astarget?: Partial<AsTargetOptions>;
}

export class LocalFileSystemDuplexConnector extends FileSystemDuplexConnector {
    type = 'local';

    // options
    connection: LocalFileSystemConnection;
    assource: AsSourceOptions;
    astarget: AsTargetOptions;

    constructor({ connection, assource = {}, astarget = {} }: LocalFileSystemOptions) {
        super();

        this.connection = connection;

        this.assource = merge({
            bulk_read_size: 10000,
            gzip: {
                chunkSize: 50 * 1024,
                level: zlib.constants.Z_BEST_SPEED,
            },
        }, assource);

        this.astarget = merge({
            remove_on_failure: true,
            remove_on_startup: true,
            gzip: {
                chunkSize: 50 * 1024,
                level: zlib.constants.Z_BEST_SPEED,
            },
            bulk_write_size: 50 * 1024
        }, astarget);
    }

    createWriteStream() {
        return createWriteStream(this.connection.path, {
            highWaterMark: this.astarget.bulk_write_size,
            autoClose: true,
            emitClose: true
        });
    }

    createReadStream() {
        return createReadStream(this.connection.path, { highWaterMark: this.assource.bulk_read_size });
    }

    async remove() {
        return unlinkP(this.connection.path).then(() => true).catch(() => false);
    }

    async connect() {
    }

    async close() {
    }

    options() {
        return pick(this, ['connection', 'assource', 'astarget']);
    }

    schema() {
        return schema;
    }

    async exists() {
        return accessP(this.connection.path).then(() => true).catch(() => false);
    }

    async fullname() {
        return `type: ${this.type}, path: ${this.connection.path}`;
    }
}

export interface LocalFileSystemConnectorOptions {
    connection: LocalFileSystemConnection;
}

export interface LocalFileSystemConnection {
    /**
     * The path to the archive file to create (relative to current working directory).
     */
    path: string;
}

interface AsSourceOptions {
    /**
     * The amount of bytes to read (in bson format) from the file each time.
     * The bigger the number is it will improve the performance as it will decrease the amount of reads from the disk (less io consumption).
     */
    bulk_read_size: number;

    /**
    * collections to read from the file.
    * If its empty, the filter is skipped, reading all the collections from the file.
    */
    collections?: string[];

    /**
     * options to use when extracting data from the source file
     */
    gzip: GzipOpts,
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
    * collections to write into the file.
    * If its empty, the filter is skipped, writing all the collections from the file.
    */
    collections?: string[];

    /**
     * options to use when compressing data into the target file
     */
    gzip: GzipOpts;

    /**
     * The amount of bytes to write into the file each time.
     * The bigger the number is it will improve the performance as it will decrease the amount of writes to the disk.
     */
    bulk_write_size: number;
}
