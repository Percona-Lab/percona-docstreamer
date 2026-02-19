package indexer

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// executeSplit performs the split command with retries for metadata lock contention
func executeSplit(ctx context.Context, adminDB *mongo.Database, ns string, middle bson.D) bool {
	cmd := bson.D{{Key: "split", Value: ns}, {Key: "middle", Value: middle}}
	for attempt := 1; attempt <= 5; attempt++ {
		err := adminDB.RunCommand(ctx, cmd).Err()
		if err == nil {
			return true
		}

		errMsg := err.Error()
		// If already split at this point, or invalid bounds, treat as success to avoid stalling
		if strings.Contains(errMsg, "is a boundary") || strings.Contains(errMsg, "already") {
			return true
		}

		// Handle MongoDB metadata lock contention
		if strings.Contains(errMsg, "lock") || strings.Contains(errMsg, "busy") || strings.Contains(errMsg, "ConflictingOperationInProgress") {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			continue
		}

		break
	}
	return false
}

func ApplyPreSplit(ctx context.Context, targetClient *mongo.Client, ns string, rule *config.ShardRule, collInfo discover.CollectionInfo) {
	if rule.PreSplitStrategy == "" || rule.PreSplitStrategy == "none" {
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Applying pre-split strategy: %s", ns, rule.PreSplitStrategy), 0)

	switch rule.PreSplitStrategy {
	case "composite_uuid_oid":
		preSplitCompositeUUID(ctx, targetClient, ns, rule.ShardKey, rule.UUIDField, rule.OIDField)
	case "hex":
		preSplitHex(ctx, targetClient, ns, rule.ShardKey)
	case "range_manual":
		preSplitRangeManual(ctx, targetClient, ns, rule, collInfo)
	case "hashed_manual":
		preSplitHashedManual(ctx, targetClient, ns, rule)
	default:
		logging.PrintWarning(fmt.Sprintf("[%s] Unknown pre-split strategy '%s'. Skipping.", ns, rule.PreSplitStrategy), 0)
	}
}

// preSplitRangeManual splits based on User Config (Min/Max) + Dynamic Size
// supports: int64, date, string, uuid, objectid
func preSplitRangeManual(ctx context.Context, targetClient *mongo.Client, ns string, rule *config.ShardRule, collInfo discover.CollectionInfo) {
	if rule.SplitMin == "" || rule.SplitMax == "" {
		logging.PrintError(fmt.Sprintf("[%s] 'range_manual' requires 'split_min' and 'split_max' in config.", ns), 0)
		return
	}

	keys := parseShardKeyNames(rule.ShardKey)
	if len(keys) == 0 {
		return
	}
	primaryKey := keys[0]

	shards, _ := getShardList(ctx, targetClient)
	minChunks := int64(len(shards))
	if minChunks < 2 {
		minChunks = 2
	}

	targetChunks := int64(rule.PreSplitChunkCount)
	if targetChunks <= 0 {
		const TargetChunkSizeMB = 128
		if collInfo.Size > 0 {
			sizeBasedChunks := collInfo.Size / (TargetChunkSizeMB * 1024 * 1024)
			if sizeBasedChunks > minChunks {
				targetChunks = sizeBasedChunks
			} else {
				targetChunks = minChunks * 2
			}
		} else {
			targetChunks = minChunks * 2
		}
	}

	if targetChunks < 2 {
		targetChunks = 2
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Manual Split: Target Chunks = %d", ns, targetChunks), 0)

	adminDB := targetClient.Database("admin")
	var splitCount int32

	var wg sync.WaitGroup
	sem := make(chan struct{}, 15) // Bounded concurrency limit

	switch rule.SplitType {
	case "int64":
		minVal, _ := strconv.ParseInt(rule.SplitMin, 10, 64)
		maxVal, _ := strconv.ParseInt(rule.SplitMax, 10, 64)

		if maxVal <= minVal {
			logging.PrintError(fmt.Sprintf("[%s] Invalid range: Min >= Max", ns), 0)
			return
		}

		step := (maxVal - minVal) / targetChunks
		if step < 1 {
			step = 1
		}

		for i := int64(1); i < targetChunks; i++ {
			splitVal := minVal + (step * i)
			middle := bson.D{{Key: primaryKey, Value: splitVal}}
			for j := 1; j < len(keys); j++ {
				middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}

	case "date":
		layout := time.RFC3339
		if len(rule.SplitMin) == 10 {
			layout = "2006-01-02"
		}

		minTime, err1 := time.Parse(layout, rule.SplitMin)
		maxTime, err2 := time.Parse(layout, rule.SplitMax)

		if err1 != nil || err2 != nil {
			logging.PrintError(fmt.Sprintf("[%s] Failed to parse dates: %v, %v", ns, err1, err2), 0)
			return
		}

		minMs := minTime.UnixMilli()
		maxMs := maxTime.UnixMilli()

		if maxMs <= minMs {
			logging.PrintError(fmt.Sprintf("[%s] Invalid date range: Min >= Max", ns), 0)
			return
		}

		step := (maxMs - minMs) / targetChunks
		if step < 1 {
			step = 1
		}

		for i := int64(1); i < targetChunks; i++ {
			splitMs := minMs + (step * i)
			splitDate := bson.DateTime(splitMs)

			middle := bson.D{{Key: primaryKey, Value: splitDate}}
			for j := 1; j < len(keys); j++ {
				middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}

	case "uuid":
		minClean := strings.ReplaceAll(rule.SplitMin, "-", "")
		maxClean := strings.ReplaceAll(rule.SplitMax, "-", "")

		minBytes, err1 := hex.DecodeString(minClean)
		maxBytes, err2 := hex.DecodeString(maxClean)

		if err1 != nil || err2 != nil || len(minBytes) != 16 || len(maxBytes) != 16 {
			logging.PrintError(fmt.Sprintf("[%s] Invalid UUID hex strings provided.", ns), 0)
			return
		}

		minInt := new(big.Int).SetBytes(minBytes)
		maxInt := new(big.Int).SetBytes(maxBytes)

		if maxInt.Cmp(minInt) <= 0 {
			logging.PrintError(fmt.Sprintf("[%s] Invalid UUID range: Min >= Max", ns), 0)
			return
		}

		rangeSize := new(big.Int).Sub(maxInt, minInt)
		step := new(big.Int).Div(rangeSize, big.NewInt(targetChunks))

		for i := int64(1); i < targetChunks; i++ {
			offset := new(big.Int).Mul(step, big.NewInt(i))
			splitInt := new(big.Int).Add(minInt, offset)

			splitBytes := splitInt.Bytes()
			if len(splitBytes) < 16 {
				pad := make([]byte, 16-len(splitBytes))
				splitBytes = append(pad, splitBytes...)
			} else if len(splitBytes) > 16 {
				splitBytes = splitBytes[len(splitBytes)-16:]
			}

			binVal := bson.Binary{Data: splitBytes, Subtype: 0x04}
			middle := bson.D{{Key: primaryKey, Value: binVal}}
			for j := 1; j < len(keys); j++ {
				middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}

	case "objectid":
		if len(rule.SplitMin) != 24 || len(rule.SplitMax) != 24 {
			logging.PrintError(fmt.Sprintf("[%s] ObjectId range must be 24-char hex strings.", ns), 0)
			return
		}

		minBytes, err1 := hex.DecodeString(rule.SplitMin)
		maxBytes, err2 := hex.DecodeString(rule.SplitMax)

		if err1 != nil || err2 != nil {
			logging.PrintError(fmt.Sprintf("[%s] Invalid ObjectId hex.", ns), 0)
			return
		}

		minInt := new(big.Int).SetBytes(minBytes)
		maxInt := new(big.Int).SetBytes(maxBytes)

		if maxInt.Cmp(minInt) <= 0 {
			logging.PrintError(fmt.Sprintf("[%s] Invalid ObjectId range: Min >= Max", ns), 0)
			return
		}

		rangeSize := new(big.Int).Sub(maxInt, minInt)
		step := new(big.Int).Div(rangeSize, big.NewInt(targetChunks))

		for i := int64(1); i < targetChunks; i++ {
			offset := new(big.Int).Mul(step, big.NewInt(i))
			splitInt := new(big.Int).Add(minInt, offset)

			splitBytes := splitInt.Bytes()
			if len(splitBytes) < 12 {
				pad := make([]byte, 12-len(splitBytes))
				splitBytes = append(pad, splitBytes...)
			} else if len(splitBytes) > 12 {
				splitBytes = splitBytes[len(splitBytes)-12:]
			}

			var oid [12]byte
			copy(oid[:], splitBytes)
			oidVal := bson.ObjectID(oid)

			middle := bson.D{{Key: primaryKey, Value: oidVal}}
			for j := 1; j < len(keys); j++ {
				middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}

	case "string":
		minStr := rule.SplitMin
		maxStr := rule.SplitMax

		if maxStr <= minStr {
			logging.PrintError(fmt.Sprintf("[%s] Invalid String range: Min >= Max", ns), 0)
			return
		}

		maxLen := len(minStr)
		if len(maxStr) > maxLen {
			maxLen = len(maxStr)
		}

		minBytes := make([]byte, maxLen)
		copy(minBytes, []byte(minStr))

		maxBytes := make([]byte, maxLen)
		copy(maxBytes, []byte(maxStr))

		minInt := new(big.Int).SetBytes(minBytes)
		maxInt := new(big.Int).SetBytes(maxBytes)

		rangeSize := new(big.Int).Sub(maxInt, minInt)
		step := new(big.Int).Div(rangeSize, big.NewInt(targetChunks))

		for i := int64(1); i < targetChunks; i++ {
			offset := new(big.Int).Mul(step, big.NewInt(i))
			splitInt := new(big.Int).Add(minInt, offset)

			splitBytes := splitInt.Bytes()
			splitVal := string(splitBytes)

			middle := bson.D{{Key: primaryKey, Value: splitVal}}
			for j := 1; j < len(keys); j++ {
				middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}

	default:
		logging.PrintWarning(fmt.Sprintf("[%s] Manual split type '%s' not supported.", ns, rule.SplitType), 0)
	}

	wg.Wait()
	logging.PrintSuccess(fmt.Sprintf("[%s] Manual split complete. Created %d chunks.", ns, atomic.LoadInt32(&splitCount)), 0)
}

func preSplitHashedManual(ctx context.Context, targetClient *mongo.Client, ns string, rule *config.ShardRule) {
	keys := parseShardKeyNames(rule.ShardKey)
	if len(keys) == 0 {
		return
	}
	primaryKey := keys[0]

	shards, _ := getShardList(ctx, targetClient)
	numShards := int64(len(shards))
	if numShards < 1 {
		numShards = 1
	}

	targetChunks := int64(rule.PreSplitChunkCount)
	if targetChunks <= 0 {
		targetChunks = numShards * 2
	}
	if targetChunks < 2 {
		targetChunks = 2
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Hashed Split: Target Chunks = %d (Shards: %d)", ns, targetChunks, numShards), 0)

	adminDB := targetClient.Database("admin")
	var splitCount int32

	var wg sync.WaitGroup
	sem := make(chan struct{}, 15)

	minVal := big.NewInt(math.MinInt64)
	maxVal := big.NewInt(math.MaxInt64)
	rangeSize := new(big.Int).Sub(maxVal, minVal)

	step := new(big.Int).Div(rangeSize, big.NewInt(targetChunks))

	for i := int64(1); i < targetChunks; i++ {
		offset := new(big.Int).Mul(step, big.NewInt(i))
		splitBig := new(big.Int).Add(minVal, offset)
		splitVal := splitBig.Int64()

		middle := bson.D{{Key: primaryKey, Value: splitVal}}
		for j := 1; j < len(keys); j++ {
			middle = append(middle, bson.E{Key: keys[j], Value: bson.MinKey{}})
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(mid bson.D) {
			defer wg.Done()
			defer func() { <-sem }()
			if executeSplit(ctx, adminDB, ns, mid) {
				atomic.AddInt32(&splitCount, 1)
			}
		}(middle)
	}

	wg.Wait()
	logging.PrintSuccess(fmt.Sprintf("[%s] Hashed manual split complete. Created %d chunks.", ns, atomic.LoadInt32(&splitCount)), 0)
}

func preSplitHex(ctx context.Context, targetClient *mongo.Client, ns, shardKeyStr string) {
	keys := parseShardKeyNames(shardKeyStr)
	if len(keys) == 0 {
		return
	}
	keyField := keys[0]

	logging.PrintStep(fmt.Sprintf("[%s] Starting Hex Pre-Split (0-F) on key '%s'", ns, keyField), 0)

	adminDB := targetClient.Database("admin")
	var splitCount int32

	var wg sync.WaitGroup
	sem := make(chan struct{}, 15)
	hexDigits := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

	for _, char := range hexDigits {
		middle := bson.D{{Key: keyField, Value: char}}
		for i := 1; i < len(keys); i++ {
			middle = append(middle, bson.E{Key: keys[i], Value: bson.MinKey{}})
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(mid bson.D) {
			defer wg.Done()
			defer func() { <-sem }()
			if executeSplit(ctx, adminDB, ns, mid) {
				atomic.AddInt32(&splitCount, 1)
			}
		}(middle)
	}

	wg.Wait()
	logging.PrintSuccess(fmt.Sprintf("[%s] Hex split complete. Created %d chunks.", ns, atomic.LoadInt32(&splitCount)), 0)
}

func preSplitCompositeUUID(ctx context.Context, targetClient *mongo.Client, ns, shardKeyStr, uuidField, oidField string) {
	keys := parseShardKeyNames(shardKeyStr)
	if len(keys) == 0 {
		return
	}

	if uuidField == "" && oidField == "" {
		logging.PrintError(fmt.Sprintf("[%s] composite_uuid_oid requires fields in config.", ns), 0)
		return
	}

	logging.PrintStep(fmt.Sprintf("[%s] Starting Composite UUID/OID Split.", ns), 0)
	adminDB := targetClient.Database("admin")
	var splitCount int32

	var wg sync.WaitGroup
	sem := make(chan struct{}, 15)
	hexDigits := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

	if uuidField != "" && oidField != "" {
		for _, digit := range hexDigits {
			oidHex := digit + "00000000000000000000000"
			oid, _ := bson.ObjectIDFromHex(oidHex)
			middle := bson.D{}
			for _, k := range keys {
				if k == uuidField {
					middle = append(middle, bson.E{Key: k, Value: nil})
				} else if k == oidField {
					middle = append(middle, bson.E{Key: k, Value: oid})
				} else {
					middle = append(middle, bson.E{Key: k, Value: bson.MinKey{}})
				}
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(mid bson.D) {
				defer wg.Done()
				defer func() { <-sem }()
				if executeSplit(ctx, adminDB, ns, mid) {
					atomic.AddInt32(&splitCount, 1)
				}
			}(middle)
		}
	}

	if uuidField != "" {
		for _, i := range hexDigits {
			for _, j := range hexDigits {
				prefix := i + j
				if prefix == "00" {
					continue
				}
				uuidHex := prefix + "000000000000000000000000000000"
				uuidBytes, _ := hex.DecodeString(uuidHex)
				uuidVal := bson.Binary{Data: uuidBytes, Subtype: 0x04}
				middle := bson.D{}
				for _, k := range keys {
					if k == uuidField {
						middle = append(middle, bson.E{Key: k, Value: uuidVal})
					} else {
						middle = append(middle, bson.E{Key: k, Value: bson.MinKey{}})
					}
				}

				wg.Add(1)
				sem <- struct{}{}
				go func(mid bson.D) {
					defer wg.Done()
					defer func() { <-sem }()
					if executeSplit(ctx, adminDB, ns, mid) {
						atomic.AddInt32(&splitCount, 1)
					}
				}(middle)
			}
		}
	}

	wg.Wait()
	logging.PrintSuccess(fmt.Sprintf("[%s] Composite split complete. Created %d chunks.", ns, atomic.LoadInt32(&splitCount)), 0)
}

func parseShardKeyNames(shardKey string) []string {
	var names []string
	parts := strings.Split(shardKey, ",")
	for _, part := range parts {
		kv := strings.Split(strings.TrimSpace(part), ":")
		if len(kv) > 0 {
			names = append(names, strings.TrimSpace(kv[0]))
		}
	}
	return names
}
