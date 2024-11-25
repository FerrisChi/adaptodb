package controller

import (
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
	"sort"
)

type Controller struct {
	metadata *metadata.Metadata `yaml:"metadata" json:"metadata"`
	pb.UnimplementedControllerServer
	strategy string
	operator *Operator
}

func NewController(metadata *metadata.Metadata) *Controller {
	return &Controller{metadata: metadata}
}

func (sc *Controller) Start() {
	// Implement shard management logic here
}

func (sc *Controller) Stop() {
	// Implement shutdown logic here
}

// UpdateSchedule handles load updates and performs shard rebalancing
func (sc *Controller) UpdateSchedule(ctx context.Context, req *pb.UpdateScheduleRequest) (*pb.UpdateScheduleResponse, error) {
	log.Println("Received new schedule update request")

	// Extract shard load information from request
	protoSchedule := req.GetSchedule()
	if len(protoSchedule) > 0 {
		schedules := make([]*schema.Schedule, 0, len(protoSchedule))
		for _, shard := range protoSchedule {
			schedule := &schema.Schedule{
				ShardID: shard.GetShardId(),
			}
			for _, keyRange := range shard.GetKeyRanges() {
				schedule.KeyRanges = append(schedule.KeyRanges, schema.KeyRange{
					Start: keyRange.GetStart(),
					End:   keyRange.GetEnd(),
				})
			}
			schedules = append(schedules, schedule)
		}

		// Perform data migration asynchronously
		go func(newSchedule []*schema.Schedule) {
			// Perform data migrations
			err := sc.performDataMigration(newSchedule)
			if err != nil {
				log.Printf("Data migration failed: %v", err)
			} else {
				log.Println("Data migration completed successfully")
			}
		}(schedules)
	}

	return &pb.UpdateScheduleResponse{
		Status:  "OK",
		Message: "Shard load update received. Processing asynchronously.",
	}, nil
}

// MigrationTask represents data movement between two shards

func (sc *Controller) performDataMigration(schedule []*schema.Schedule) error {
	oldSchedule, err := sc.metadata.GetAllShardKeyRanges()
	if err != nil {
		return fmt.Errorf("failed to get old schedule: %v", err)
	}

	// Map to store migrations between shard pairs
	migrations := make(map[string]*MigrationTask)

	// Gather all migrations between shard pairs
	for _, newShard := range schedule {
		newShardID := newShard.ShardID
		newRanges := newShard.KeyRanges

		for oldShardID, oldRanges := range oldSchedule {
			if oldShardID == newShardID {
				continue // Skip if same shard
			}

			// Get shard info once per shard pair
			shards := sc.metadata.GetShardBatch([]uint64{oldShardID, newShardID})
			if len(shards) != 2 || shards[0].Members == nil || shards[1].Members == nil {
				return fmt.Errorf("failed to get valid shard info for shards %d and %d", oldShardID, newShardID)
			}

			// Collect all intersecting ranges for this shard pair
			var intersections []schema.KeyRange
			for _, oldRange := range oldRanges {
				for _, newRange := range newRanges {
					if intersection, overlaps := getKeyRangeIntersection(oldRange, newRange); overlaps {
						intersections = append(intersections, intersection)
					}
				}
			}

			// Create migration task only if there are intersections
			if len(intersections) > 0 {
				key := fmt.Sprintf("%d->%d", oldShardID, newShardID)
				if _, exists := migrations[key]; !exists {
					migrations[key] = &MigrationTask{
						fromShardInfo: shards[0],
						toShardInfo:   shards[1],
						ranges:        intersections,
					}
				}
			}
		}
	}

	// Consolidate overlapping ranges for each migration
	for _, task := range migrations {
		task.ranges = consolidateKeyRanges(task.ranges)
	}

	return nil
}

// Helper function to calculate intersection of two key ranges
func getKeyRangeIntersection(r1, r2 schema.KeyRange) (schema.KeyRange, bool) {
	start := r1.Start
	if r2.Start > start {
		start = r2.Start
	}

	end := r1.End
	if r2.End < end {
		end = r2.End
	}

	// Check if there is a valid intersection
	if start >= end {
		return schema.KeyRange{}, false
	}

	return schema.KeyRange{
		Start: start,
		End:   end,
	}, true
}

// Helper function to consolidate overlapping key ranges
func consolidateKeyRanges(ranges []schema.KeyRange) []schema.KeyRange {
	if len(ranges) <= 1 {
		return ranges
	}

	// Sort ranges by start key
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Start < ranges[j].Start
	})

	result := make([]schema.KeyRange, 0)
	current := ranges[0]

	for i := 1; i < len(ranges); i++ {
		if current.End >= ranges[i].Start {
			// Ranges overlap, merge them
			if ranges[i].End > current.End {
				current.End = ranges[i].End
			}
		} else {
			// No overlap, add current to result and start new current
			result = append(result, current)
			current = ranges[i]
		}
	}
	result = append(result, current)

	return result
}
