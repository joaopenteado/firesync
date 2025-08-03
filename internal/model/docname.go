package model

import (
	"fmt"
	"strings"

	"cloud.google.com/go/firestore"
)

type DocumentName struct {
	ProjectID  string
	DatabaseID string
	Path       string
}

// NewDocumentFromPath creates a new Document from a Firestore document path.
// The path must be in the format `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
func NewDocumentFromPath(path string) *DocumentName {
	const (
		projectsPrefix = "projects/"
		databasesInfix = "/databases/"
		documentsInfix = "/documents/"
	)

	pjIdx := strings.Index(path, projectsPrefix)
	if pjIdx != 0 {
		return nil
	}

	dbIdx := strings.Index(path[len(projectsPrefix):], databasesInfix)
	if dbIdx == -1 {
		return nil
	}
	dbIdx += len(projectsPrefix)

	docIdx := strings.Index(path[dbIdx+len(databasesInfix):], documentsInfix)
	if docIdx == -1 {
		return nil
	}
	docIdx += dbIdx + len(databasesInfix)

	projectID := path[len(projectsPrefix):dbIdx]
	databaseID := path[dbIdx+len(databasesInfix) : docIdx]
	docPath := path[docIdx+len(documentsInfix):]

	if projectID == "" || databaseID == "" || docPath == "" {
		return nil
	}

	return &DocumentName{
		ProjectID:  projectID,
		DatabaseID: databaseID,
		Path:       docPath,
	}
}

func (d *DocumentName) Ref(db *firestore.Client) *firestore.DocumentRef {
	return db.Doc(d.Path)
}

func (d *DocumentName) TombstoneRef(db *firestore.Client) *firestore.DocumentRef {
	return db.Collection(TombstoneCollection).Doc(TombstoneID(d.Path))
}

func (d *DocumentName) String() string {
	return fmt.Sprintf("projects/%s/databases/%s/documents/%s", d.ProjectID, d.DatabaseID, d.Path)
}
