# [ditto](https://github.com/lihaibh/ditto)

Dump and restore DB content between data sources.

## Installation
```sh
npm install @ditto/core --save
```

## Write your own connector
If a connector is not implemented, it is very simple to write your own using @ditto/sdk to solve common connector problems.
If your connector can be used by others, I advise you to publish it as a package to ditto repository.

## Usage
choose the dest & target you wish to perform the migration on.
install the right libs, for example: @ditto/mongodb for write & read data from and to mongodb.
*dump mongodb database to a local file*
``` sh
npm install @ditto/core @ditto/mongodb @ditto/filesystem-local --save
```
```typescript
import { Transferer } from '@ditto/core';
import { MongoDBConnector } from '@ditto/mongodb';
import { LocalFileSystemConnector } from '@ditto/filesystem-local';

async function dumpMongo2Localfile() {
    const mongo_connector = new MongoDBConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const localfile_connector = new LocalFileSystemConnector({
        connection: {
            path: './backup.tar',
        },
    });

    const transferer = new Transferer({
        source: mongo_connector,
        targets: [localfile_connector],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```

*restore mongodb database from a local file*
``` sh
npm install @ditto/core @ditto/mongodb @ditto/filesystem-local --save
```
```typescript
import { Transferer } from '@ditto/core';
import { MongoDBConnector } from '@ditto/mongodb';
import { LocalFileSystemConnector } from '@ditto/filesystem-local';

async function restoreLocalfile2Mongo() {
    const mongo_connector = new MongoDBConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const localfile_connector = new LocalFileSystemConnector({
        connection: {
            path: './backup.tar',
        },
    });

    const transferer = new Transferer({
        source: localfile_connector,
        targets: [mongo_connector],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```


*copy mongodb database to another mongodb database*
``` sh
npm install @ditto/core @ditto/mongodb --save
```
```typescript
import { Transferer } from '@ditto/core';
import { MongoDBConnector } from '@ditto/mongodb';

async function copyMongo2Mongo() {
    const mongo_connector_1 = new MongoDBConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const mongo_connector_2 = new MongoDBConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const mongo_connector_3 = new MongoDBConnector({
        connection: {
            uri: `mongodb://<username>:<password>@<hostname>:<port>`,
            dbname: '<database-name>',
        },
    });

    const transferer = new Transferer({
        source: mongo_connector_1,
        targets: [mongo_connector_2, mongo_connector_3],
    });

    for await (const { total, write } of transferer) {
        console.log(`remaining bytes to write: ${total - write}`);
    }
}
```