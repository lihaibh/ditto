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

export function hasRegexesMatch(regexes: string[] | undefined, name: string) {
    return regexes?.find(reg => hasRegexMatch(reg, name)) !== undefined;
}

export function hasRegexMatch(reg: string, name: string) {
    try {
        return (new RegExp(`^${reg}$`, 'g')).exec(name)?.[0] === name;
    } catch (err) {
        return false;
    }
}