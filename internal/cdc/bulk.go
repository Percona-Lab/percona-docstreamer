package cdc

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Batch struct {
	Models     []mongo.WriteModel
	Keys       []bson.D
	LastTS     bson.Timestamp
	Inserts    int64
	Updates    int64
	Deletes    int64
	EventCount int64
}

type ShardKeyInfo struct {
	Key  string
	Path []string
}

type ShardKeyProvider func(ctx context.Context, ns string) ([]string, error)

type BulkWriter struct {
	targetClient *mongo.Client
	batchSize    int
	batches      map[string]*Batch
	keyProvider  ShardKeyProvider
}

func NewBulkWriter(target *mongo.Client, batchSize int, provider ShardKeyProvider) *BulkWriter {
	return &BulkWriter{
		targetClient: target,
		batchSize:    batchSize,
		batches:      make(map[string]*Batch),
		keyProvider:  provider,
	}
}

func (b *BulkWriter) getShardKeyInfo(ns string) ([]ShardKeyInfo, error) {
	keys, err := b.keyProvider(context.Background(), ns)
	if err != nil {
		return nil, err
	}

	info := make([]ShardKeyInfo, len(keys))
	for i, key := range keys {
		info[i] = ShardKeyInfo{
			Key:  key,
			Path: strings.Split(key, "."),
		}
	}
	return info, nil
}

func GetNestedValue(doc interface{}, path []string) (interface{}, bool) {
	if doc == nil {
		return nil, false
	}
	current := doc
	for _, part := range path {
		switch v := current.(type) {
		case bson.M:
			if val, exists := v[part]; exists {
				current = val
			} else {
				return nil, false
			}
		case map[string]interface{}:
			if val, exists := v[part]; exists {
				current = val
			} else {
				return nil, false
			}
		case bson.D:
			found := false
			for _, e := range v {
				if e.Key == part {
					current = e.Value
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

func (b *BulkWriter) createFilter(ns string, event *ChangeEvent) (bson.D, error) {
	shardKeys, err := b.getShardKeyInfo(ns)
	if err != nil {
		return nil, err
	}

	filter := make(bson.D, 0, len(shardKeys)+1)

	for _, sk := range shardKeys {
		if sk.Key == "_id" {
			continue
		}
		if val, ok := GetNestedValue(event.FullDocument, sk.Path); ok {
			filter = append(filter, bson.E{Key: sk.Key, Value: val})
		} else if val, ok := GetNestedValue(event.DocumentKey, sk.Path); ok {
			filter = append(filter, bson.E{Key: sk.Key, Value: val})
		}
	}

	if idVal, ok := event.DocumentKey["_id"]; ok {
		filter = append(filter, bson.E{Key: "_id", Value: idVal})
	}
	return filter, nil
}

// AddEvent adds an event to the batch.
func (b *BulkWriter) AddEvent(event *ChangeEvent) (bool, bool, error) {
	ns := event.Ns()
	filter, err := b.createFilter(ns, event)
	if err != nil {
		return false, false, err
	}

	if b.batches[ns] == nil {
		batch := &Batch{
			Models: []mongo.WriteModel{},
			Keys:   []bson.D{},
		}
		b.batches[ns] = batch
	}

	batch := b.batches[ns]
	var model mongo.WriteModel

	// --- Shard Key Update Detection ---
	isShardKeyUpdated := false
	if event.OperationType == Update && event.UpdateFields != nil {
		if updated, ok := event.UpdateFields["updatedFields"]; ok && updated != nil {
			shardKeys, err := b.getShardKeyInfo(ns)
			if err != nil {
				return false, false, err
			}

			switch v := updated.(type) {
			case bson.M:
				for _, sk := range shardKeys {
					if _, exists := v[sk.Key]; exists {
						isShardKeyUpdated = true
						break
					}
				}
			case bson.D:
				for _, sk := range shardKeys {
					for _, e := range v {
						if e.Key == sk.Key || strings.HasPrefix(e.Key, sk.Key+".") {
							isShardKeyUpdated = true
							break
						}
					}
				}
			}
		}
	}

	switch event.OperationType {
	case Insert:
		if event.FullDocument != nil {
			model = mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(event.FullDocument).SetUpsert(true)
			batch.Inserts++
		}
	case Replace:
		if event.FullDocument != nil {
			model = mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(event.FullDocument).SetUpsert(true)
			batch.Updates++
		}
	case Update:
		if isShardKeyUpdated && event.FullDocument != nil {
			// Split Shard Key Update
			delFilter := bson.D{}
			shardKeys, _ := b.getShardKeyInfo(ns)

			// Build delete filter using ONLY DocumentKey
			for _, sk := range shardKeys {
				if val, ok := GetNestedValue(event.DocumentKey, sk.Path); ok {
					delFilter = append(delFilter, bson.E{Key: sk.Key, Value: val})
				}
			}
			if idVal, ok := event.DocumentKey["_id"]; ok {
				delFilter = append(delFilter, bson.E{Key: "_id", Value: idVal})
			}

			// Queue Delete + Insert
			delModel := mongo.NewDeleteOneModel().SetFilter(delFilter)
			insModel := mongo.NewInsertOneModel().SetDocument(event.FullDocument)

			batch.Models = append(batch.Models, delModel, insModel)
			batch.Keys = append(batch.Keys, delFilter, filter)
			batch.Updates++
			batch.EventCount++

			if event.ClusterTime.T > batch.LastTS.T ||
				(event.ClusterTime.T == batch.LastTS.T && event.ClusterTime.I > batch.LastTS.I) {
				batch.LastTS = event.ClusterTime
			}
			return true, len(batch.Models) >= b.batchSize, nil

		} else if event.UpdateFields != nil {
			updateDoc := bson.D{}
			if updated, ok := event.UpdateFields["updatedFields"]; ok && updated != nil {
				isEmpty := false
				if m, isM := updated.(bson.M); isM && len(m) == 0 {
					isEmpty = true
				} else if d, isD := updated.(bson.D); isD && len(d) == 0 {
					isEmpty = true
				}
				if !isEmpty {
					updateDoc = append(updateDoc, bson.E{Key: "$set", Value: updated})
				}
			}
			if removed, ok := event.UpdateFields["removedFields"].(bson.A); ok && len(removed) > 0 {
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
				model = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(updateDoc).SetUpsert(false)
				batch.Updates++
			} else if event.FullDocument != nil {
				model = mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(event.FullDocument).SetUpsert(true)
				batch.Updates++
			}
		}
	case Delete:
		model = mongo.NewDeleteOneModel().SetFilter(filter)
		batch.Deletes++
	}

	if model != nil {
		batch.Models = append(batch.Models, model)
		batch.Keys = append(batch.Keys, filter)
		batch.EventCount++
		if event.ClusterTime.T > batch.LastTS.T ||
			(event.ClusterTime.T == batch.LastTS.T && event.ClusterTime.I > batch.LastTS.I) {
			batch.LastTS = event.ClusterTime
		}
		return true, len(batch.Models) >= b.batchSize, nil
	}

	return false, len(batch.Models) >= b.batchSize, nil
}

func (b *BulkWriter) ExtractBatches() map[string]*Batch {
	current := b.batches
	b.batches = make(map[string]*Batch)
	return current
}
