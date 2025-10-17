import * as React from 'react';

export default function ExComp() {
    const onClick = async () => {
        try {
            throw new Error('boom');
        } catch (e) {
            console.error(e);
        }
    };
    return <button onClick={onClick}>Click</button>;
}
