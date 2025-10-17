export interface IFoo<T> {
    doIt(v: T): T;
}

export class ImplFoo<T> implements IFoo<T> {
    doIt(v: T): T { return v }
}
