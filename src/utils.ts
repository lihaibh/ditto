import { Observable } from "rxjs";

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