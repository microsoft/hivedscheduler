// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package common

import (
	"fmt"
	"k8s.io/klog"
)

type Empty struct{}
type T interface{}

type Set struct {
	items map[T]Empty
}

func NewSet(items ...T) Set {
	s := Set{items: map[T]Empty{}}
	for _, item := range items {
		s.Add(item)
	}
	return s
}

func (s Set) Contains(item T) bool {
	_, exists := s.items[item]
	return exists
}

func (s Set) Add(item T) Set {
	s.items[item] = Empty{}
	return s
}

func (s Set) Delete(item T) Set {
	delete(s.items, item)
	return s
}

func (s Set) IsEmpty() bool {
	return len(s.items) == 0
}

func (s Set) Items() map[T]Empty {
	return s.items
}

func (s Set) String() string {
	ss := make([]string, len(s.items))
	n := int32(0)
	for item := range s.items {
		ss[n] = fmt.Sprintf("%v", item)
		n++
	}
	return ToJson(ss)
}

type ImmutableSet struct {
	set Set
}

func NewImmutableSet(items ...T) ImmutableSet {
	return ImmutableSet{set: NewSet(items...)}
}

func (s ImmutableSet) Contains(item T) bool {
	return s.set.Contains(item)
}

type KlogWriter struct{}

func (w KlogWriter) Write(data []byte) (n int, err error) {
	klog.InfoDepth(1, string(data))
	return len(data), nil
}
