package sample

import "golang.org/x/exp/constraints"

type List[T any] struct {
    items []T
}

func Id[T any](v T) T { return v }
