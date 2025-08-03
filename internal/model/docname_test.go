package model

import (
	"reflect"
	"testing"
)

func TestNewDocumentFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want *DocumentName
	}{
		{
			name: "valid path",
			path: "projects/abc/databases/def/documents/users/123",
			want: &DocumentName{ProjectID: "abc", DatabaseID: "def", Path: "users/123"},
		},
		{
			name: "valid path with multiple segments",
			path: "projects/p/databases/d/documents/col/doc/col2/doc2",
			want: &DocumentName{ProjectID: "p", DatabaseID: "d", Path: "col/doc/col2/doc2"},
		},
		{
			name: "empty path",
			path: "",
			want: nil,
		},
		{
			name: "invalid prefix",
			path: "foo/abc/databases/def/documents/users/123",
			want: nil,
		},
		{
			name: "missing projects prefix",
			path: "abc/databases/def/documents/users/123",
			want: nil,
		},
		{
			name: "missing databases infix",
			path: "projects/abc/documents/users/123",
			want: nil,
		},
		{
			name: "missing documents infix",
			path: "projects/abc/databases/def/users/123",
			want: nil,
		},
		{
			name: "empty project id",
			path: "projects//databases/def/documents/users/123",
			want: nil,
		},
		{
			name: "empty database id",
			path: "projects/abc/databases//documents/users/123",
			want: nil,
		},
		{
			name: "empty document path",
			path: "projects/abc/databases/def/documents/",
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := NewDocumentFromPath(test.path)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("NewDocumentFromPath(%q) = %v, want %v", test.path, got, test.want)
			}
		})
	}
}
