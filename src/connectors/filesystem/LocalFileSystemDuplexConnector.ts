import { FileSystemDuplexConnector } from "./FileSystemDuplexConnector";
import { GzipOpts } from "../../contracts";
import { createReadStream, createWriteStream, access, unlink } from "fs";
import { Readable, Writable } from "stream";
import * as joi from "joi";
import { pick } from "lodash";
import { promisify } from 'util';

const accessP = promisify(access);
const unlinkP = promisify(unlink);

const schema = joi.object({
    connection: joi.object({
        path: joi.string().required()
    }).required(),
    remove_on_failure: joi.boolean().optional(),
    gzip: joi.object({
        flush: joi.number().optional(),
        finishFlush: joi.number().optional(),
        chunkSize: joi.number().optional(),
        windowBits: joi.number().optional(),
        level: joi.number().optional(),
        memLevel: joi.number().optional(),
        strategy: joi.number().optional(),
    }).optional()
});

export interface LocalFileSystemOptions {
    connection: LocalFileSystemConnection;
    gzip?: GzipOpts;
    remove_on_failure?: boolean;
    remove_on_startup?: boolean;
}

export class LocalFileSystemDuplexConnector extends FileSystemDuplexConnector {
    name = 'local';
    connection: LocalFileSystemConnection;

    // extra options
    remove_on_failure: boolean;
    remove_on_startup: boolean;
    gzip: GzipOpts;

    constructor({ connection, remove_on_failure = false, remove_on_startup = false, gzip = { chunkSize: 5000 } }: LocalFileSystemOptions) {
        super();

        this.connection = connection;
        this.remove_on_failure = remove_on_failure;
        this.remove_on_startup = remove_on_startup;
        this.gzip = gzip;
    }

    createWriteStream() {
        return createWriteStream(this.connection.path, {
            autoClose: true,
            emitClose: true
        });
    }

    createReadStream(batch_size?: number) {
        return createReadStream(this.connection.path, { highWaterMark: batch_size });
    }

    async remove() {
        return unlinkP(this.connection.path).then(() => true).catch(() => false);
    }

    async connect() {
    }

    async close() {
    }

    options() {
        return pick(this, ['remove_on_failure', 'connection', 'gzip']);
    }

    schema() {
        return schema;
    }

    async exists() {
        return accessP(this.connection.path).then(() => true).catch(() => false);
    }

    async fullname() {
        return this.name;
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