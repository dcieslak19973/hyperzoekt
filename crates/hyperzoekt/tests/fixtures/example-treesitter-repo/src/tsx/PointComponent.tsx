// TSX example
// @ts-nocheck
import * as React from 'react';

interface PointProps {
    x: number;
    y: number;
}

const PointComponent = ({ x, y }: PointProps) => {
    return (
        <div>
            <span>{x}</span>
            <span>{y}</span>
        </div>
    );
};

export default PointComponent;
