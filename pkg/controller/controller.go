package controller

import (
	pb "adaptodb/pkg/controller/proto"
	"adaptodb/pkg/metadata"
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
)

type Controller struct {
	metadata *metadata.Metadata
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
		schedule := make([]*schema.ShardMetadata, 0, len(protoSchedule))
		for _, shard := range protoSchedule {
			schedule = append(schedule, &schema.ShardMetadata{
				ShardID:  shard.ShardId,
				KeyRange: schema.KeyRange{Start: shard.KeyRange.Start, End: shard.KeyRange.End},
			})
		}

		// Perform data migration asynchronously
		go func(newSchedule []*schema.ShardMetadata) {
			// Perform data migrations
			err := sc.performDataMigration(newSchedule)
			if err != nil {
				log.Printf("Data migration failed: %v", err)
			} else {
				log.Println("Data migration completed successfully")
			}
		}(schedule)
	}

	return &pb.UpdateScheduleResponse{
		Status:  "OK",
		Message: "Shard load update received. Processing asynchronously.",
	}, nil
}

// MigrationTask represents data movement between two shards

func (sc *Controller) performDataMigration(schedule []*schema.ShardMetadata) error {
	oldSchedule, err := sc.metadata.GetAllShardKeyRanges()
	if err != nil {
		return fmt.Errorf("failed to get old schedule: %v", err)
	}

	// Map to store migrations between shard pairs
	migrations := make(map[string]*MigrationTask)

	// Gather all migrations between shard pairs
	for _, newShard := range schedule {
		newShardID := newShard.ShardID
		newRange := newShard.KeyRange

		for oldShardID, oldRanges := range oldSchedule {
			if oldShardID == newShardID {
				continue // Skip if same shard
			}

			for _, oldRange := range oldRanges {
				if rangesOverlap(oldRange, newRange) {
					intersection := calculateIntersection(oldRange, newRange)

					// Create key for migration pair
					key := fmt.Sprintf("%v->%v", oldShardID, newShardID)

					// Add to existing migration task or create new one
					if task, exists := migrations[key]; exists {
						task.ranges = append(task.ranges, intersection)
					} else {
						migrations[key] = &MigrationTask{
							sourceShardID: oldShardID,
							targetShardID: newShardID,
							ranges:        []schema.KeyRange{intersection},
						}
					}
				}
			}
		}
	}

	// Execute migrations
	for _, task := range migrations {
		log.Printf("Migrating data from shard %d to shard %d for ranges: %v",
			task.sourceShardID, task.targetShardID, task.ranges)

		err := sc.operator.migrate(task.sourceShardID, task.targetShardID, task.ranges)
		if err != nil {
			return fmt.Errorf("failed to migrate data from shard %d to shard %d: %v",
				task.sourceShardID, task.targetShardID, err)
		}
	}

	return nil
}

// rangesOverlap checks if two key ranges overlap
func rangesOverlap(range1, range2 schema.KeyRange) bool {
	return range1.Start < range2.End && range2.Start < range1.End
}

// calculateIntersection computes the overlapping range between two key ranges
func calculateIntersection(range1, range2 schema.KeyRange) schema.KeyRange {
	start := max(range1.Start, range2.Start)
	end := min(range1.End, range2.End)
	return schema.KeyRange{Start: start, End: end}
}
