package controller

import (
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
)

type Controller struct {
	metadata *metadata.Metadata `yaml:"metadata" json:"metadata"`
	pb.UnimplementedControllerServer
	operators []*Operator
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
	ops := make(map[string]*Operator)

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
				if _, exists := ops[key]; !exists {
					ops[key] = NewOperator(shards[0], shards[1], intersections, nil)
				}
			}
		}
	}
	// Consolidate overlapping ranges for each migration
	for _, op := range ops {
		op.keyRanges = schema.ConsolidateKeyRanges(op.keyRanges)
		sc.operators = append(sc.operators, op)

		ctx, cancel := context.WithTimeout(context.Background(), schema.MIGRATION_TIMEOUT)
		op.cancel = cancel
		op.done = make(chan struct{})

		// Launch migration in goroutine
		err := op.migrate(ctx)

		// Wait with timeout
		if err != nil {
			log.Printf("Migration failed: %v", err)
		} else {
			log.Printf("Migration completed")
		}

		go func(op *Operator) {
			select {
			case <-op.done:
				log.Printf("Migration finished between shards %d and %d", op.fromShard.ShardID, op.toShard.ShardID)
			case <-ctx.Done():
				log.Printf("Migration timed out, initiating cancellation")
				if err := op.cancelMigration(); err != nil {
					log.Printf("Failed to cancel migration: %v", err)
				}
			}
		}(op)
	}
	return nil
}

func (sc *Controller) findOperator(fromClusterID, toClusterID uint64) *Operator {
	for _, op := range sc.operators {
		if op.fromShard.ShardID == fromClusterID && op.toShard.ShardID == toClusterID {
			return op
		}
	}
	return nil
}

func (sc *Controller) findOperatorByTaskID(taskId uint64) *Operator {
	for _, op := range sc.operators {
		if op.taskId == taskId {
			return op
		}
	}
	return nil
}

func (sc *Controller) CancelMigrationFromNode(ctx context.Context, req *pb.CancelMigrationRequest) (*pb.CancelMigrationResponse, error) {
	log.Println("Received request to cancel migration")
	op := sc.findOperator(req.GetFromClusterID(), req.GetToClusterID())
	if op == nil {
		log.Panicln("Migration not found")
	}
	op.cancelMigration()

	return &pb.CancelMigrationResponse{
		Status: "OK",
	}, nil
}

func (sc *Controller) FinishMigration(ctx context.Context, req *pb.FinishMigrationRequest) (*pb.FinishMigrationResponse, error) {
	log.Println("Received transfer done notification")
	op := sc.findOperatorByTaskID(req.GetTaskId())
	if op == nil {
		log.Panicln("Migration not found")
	}

	err := op.finishMigration()
	if err != nil {
		return nil, err
	}

	// update metadata
	sc.metadata.UpdateMigratedKeyRanges(op.fromShard.ShardID, op.toShard.ShardID, op.keyRanges)

	return &pb.FinishMigrationResponse{
		Status: "OK",
	}, nil
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
