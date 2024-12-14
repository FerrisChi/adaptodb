package balancer

import (
	"fmt"
	"log"
)

// Enum type representing the different algorithms
type ImbalanceAlgorithm int

const (
	Relative    ImbalanceAlgorithm = iota // DetectRelativeImbalance
	Percentile                            // DetectPercentileImbalance
	Statistical                           // DetectStatisticalImbalance
)

func (a ImbalanceAlgorithm) String() string {
	switch a {
	case Relative:
		return "Relative"
	case Percentile:
		return "Percentile"
	case Statistical:
		return "Statistical"
	default:
		return "Unknown"
	}
}

// ChooseImbalanceDetections calls the appropriate imbalance detection method
// based on the specified algorithm and parameter.
// For the Relative algorithm, `param` could be the threshold factor.
// For the Percentile algorithm, `param` could be the percentile value.
// For the Statistical algorithm, `param` could be the factor (e.g., # of stddevs).
func ChooseImbalanceDetections(loads []*NodeMetrics, algo ImbalanceAlgorithm, param float64) []uint64 {
	switch algo {
	case Relative:
		return DetectRelativeImbalance(loads, param)
	case Percentile:
		return DetectPercentileImbalance(loads, param)
	case Statistical:
		return DetectStatisticalImbalance(loads, param)
	default:
		log.Printf("Unknown imbalance algorithm: %v", algo)
		return []uint64{}
	}
}

// parseAlgo converts a string to a balancer.ImbalanceAlgorithm value.
func ParseAlgo(algoStr string) (ImbalanceAlgorithm, error) {
	switch algoStr {
	case "Relative":
		return Relative, nil
	case "Percentile":
		return Percentile, nil
	case "Statistical":
		return Statistical, nil
	default:
		return 0, fmt.Errorf("unknown algorithm: %s", algoStr)
	}
}
