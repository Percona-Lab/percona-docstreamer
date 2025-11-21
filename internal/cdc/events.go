package cdc

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// OperationType is the type of MongoDB operation
type OperationType string

// Constants for MongoDB operation types
const (
	Insert        OperationType = "insert"
	Update        OperationType = "update"
	Replace       OperationType = "replace"
	Delete        OperationType = "delete"
	Drop          OperationType = "drop"
	Rename        OperationType = "rename"
	DropDatabase  OperationType = "dropDatabase"
	Invalidate    OperationType = "invalidate"
	Create        OperationType = "create"
	CreateIndexes OperationType = "createIndexes"
	DropIndexes   OperationType = "dropIndexes"
)

// ChangeEvent represents a single event from the change stream
type ChangeEvent struct {
	ID            bson.Raw       `bson:"_id"`
	OperationType OperationType  `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	FullDocument  bson.Raw       `bson:"fullDocument"`
	Namespace     Namespace      `bson:"ns"`
	DocumentKey   bson.M         `bson:"documentKey"`
	To            Namespace      `bson:"to,omitempty"`
	UpdateFields  bson.M         `bson:"updateDescription,omitempty"`
}

// Namespace holds the db and collection
type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

// Ns returns the full namespace string
func (e *ChangeEvent) Ns() string {
	return fmt.Sprintf("%s.%s", e.Namespace.Database, e.Namespace.Collection)
}

// ToWriteModel is a helper
func (e *ChangeEvent) ToWriteModel() mongo.WriteModel {
	switch e.OperationType {
	case Insert:
		return mongo.NewInsertOneModel().SetDocument(e.FullDocument)
	case Update, Replace:
		return mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: e.DocumentKey["_id"]}}).
			SetReplacement(e.FullDocument).
			SetUpsert(true)
	case Delete:
		return mongo.NewDeleteOneModel().
			SetFilter(bson.D{{Key: "_id", Value: e.DocumentKey["_id"]}})
	}
	return nil
}
