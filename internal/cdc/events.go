package cdc

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type OperationType string

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

type ChangeEvent struct {
	ID            bson.Raw       `bson:"_id"`
	OperationType OperationType  `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	FullDocument  bson.M         `bson:"fullDocument"`
	Namespace     Namespace      `bson:"ns"`
	DocumentKey   bson.M         `bson:"documentKey"`
	To            Namespace      `bson:"to,omitempty"`
	UpdateFields  bson.M         `bson:"updateDescription,omitempty"`
}

type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func (e *ChangeEvent) Ns() string {
	return fmt.Sprintf("%s.%s", e.Namespace.Database, e.Namespace.Collection)
}

func (e *ChangeEvent) ToWriteModel() mongo.WriteModel {
	switch e.OperationType {
	case Insert:
		return mongo.NewInsertOneModel().SetDocument(e.FullDocument)
	case Replace:
		return mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: e.DocumentKey["_id"]}}).
			SetReplacement(e.FullDocument).
			SetUpsert(true)
	case Update:
		if e.UpdateFields != nil {
			updateDoc := bson.D{}

			if updated, ok := e.UpdateFields["updatedFields"].(bson.M); ok && len(updated) > 0 {
				updateDoc = append(updateDoc, bson.E{Key: "$set", Value: updated})
			}

			if removed, ok := e.UpdateFields["removedFields"].(bson.A); ok && len(removed) > 0 {
				unsetDoc := bson.D{}
				for _, f := range removed {
					if fieldStr, ok := f.(string); ok {
						unsetDoc = append(unsetDoc, bson.E{Key: fieldStr, Value: ""})
					}
				}
				if len(unsetDoc) > 0 {
					updateDoc = append(updateDoc, bson.E{Key: "$unset", Value: unsetDoc})
				}
			}

			if len(updateDoc) > 0 {
				return mongo.NewUpdateOneModel().
					SetFilter(bson.D{{Key: "_id", Value: e.DocumentKey["_id"]}}).
					SetUpdate(updateDoc).
					SetUpsert(false)
			}
		}
		// Fallback if UpdateFields is empty but we have a FullDocument
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
