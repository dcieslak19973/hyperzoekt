export class Box<T extends { id: number }> {
    constructor(public value: T) { }
}

export function id<T>(v: T): T { return v }
