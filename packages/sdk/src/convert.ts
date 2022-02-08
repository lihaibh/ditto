import { Observable } from 'rxjs';
import { eachValueFrom } from 'rxjs-for-await';
import * as BSON from 'bson';
import { CollectionDocument } from './types';

const BSON_DOC_HEADER_SIZE = 4;

export function convertAsyncGeneratorToObservable<T>(iterator: AsyncGenerator<T>): Observable<T> {
    return new Observable(observer => void (async () => {
        try {
            for await (const item of iterator) {
                observer.next(item);
            }

            observer.complete();
        } catch (e) {
            observer.error(e);
        }
    })())
}

// convert chunks bytes in bson format to stream of documents
export function document$(chunk$: Observable<Buffer>): Observable<CollectionDocument> {
    return convertAsyncGeneratorToObservable(async function*() {
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
    }())
    
}

// convert stream of documents to chunks of bytes in bson format
export function chunk$() {

}
