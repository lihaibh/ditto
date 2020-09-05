# [mongodb-snapshot](https://github.com/lihaibh/mongodb-snapshot)

Dump and restore MongoDB content between data sources.

## Installation
```sh
npm install mongodb-snapshot
```

## Usage
*dump mongodb database to a local file*
```typescript
import { MongoTransferer, MongoDBDuplexConnector, LocalFileSystemDuplexConnector } from 'mongodb-snapshot';

async function dumpMongo2Localfile() {
    const mongo_connector = new MongoDBDuplexConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const localfile_connector = new LocalFileSystemDuplexConnector({
        connection: {
            path: './backup.tar',
        },
    });

    const transferer = new MongoTransferer({
        source: mongo_connector,
        targets: [localfile_connector],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```

*restore mongodb database from a local file*
```typescript
import { MongoTransferer, MongoDBDuplexConnector, LocalFileSystemDuplexConnector } from 'mongodb-snapshot';

async function restoreLocalfile2Mongo() {
    const mongo_connector = new MongoDBDuplexConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const localfile_connector = new LocalFileSystemDuplexConnector({
        connection: {
            path: './backup.tar',
        },
    });

    const transferer = new MongoTransferer({
        source: localfile_connector,
        targets: [mongo_connector],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```


*copy mongodb database to another mongodb database*
```typescript
import { MongoTransferer, MongoDBDuplexConnector } from 'mongodb-snapshot';

async function copyMongo2Mongo() {
    const mongo_connector_1 = new MongoDBDuplexConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const mongo_connector_2 = new MongoDBDuplexConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const mongo_connector_3 = new MongoDBDuplexConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const transferer = new MongoTransferer({
        source: mongo_connector_1,
        targets: [mongo_connector_2, mongo_connector_3],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```