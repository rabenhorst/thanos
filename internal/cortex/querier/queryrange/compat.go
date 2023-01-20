package queryrange

import (
	"github.com/prometheus/common/model"
)

// Following two functions are slightly modified and copied from
// https://github.com/prometheus/common/blob/846591a166358c7048ef197e84501ca688dda920/model/value_histogram.go#L141-L163

func (s SampleHistogramPair) MarshalJSON() ([]byte, error) {
	return json.Marshal(toModelSampleHistogramPair(s))
}

func (s *SampleHistogramPair) UnmarshalJSON(buf []byte) error {
	var modelSampleHistogram model.SampleHistogramPair
	if err := json.Unmarshal(buf, &modelSampleHistogram); err != nil {
		return err
	}
	fromModelSampleHistogramPair(modelSampleHistogram, s)
	return nil
}

func toModelSampleHistogramPair(s SampleHistogramPair) model.SampleHistogramPair {
	modelBuckets := make([]*model.HistogramBucket, len(s.Histogram.Buckets))

	for i, b := range s.Histogram.Buckets {
		modelBuckets[i] = &model.HistogramBucket{
			Lower:      model.FloatString(b.Lower),
			Upper:      model.FloatString(b.Upper),
			Count:      model.FloatString(b.Count),
			Boundaries: int(b.Boundaries),
		}
	}

	return model.SampleHistogramPair{
		Timestamp: model.Time(s.Timestamp),
		Histogram: model.SampleHistogram{
			Count:   model.FloatString(s.Histogram.Count),
			Sum:     model.FloatString(s.Histogram.Sum),
			Buckets: modelBuckets,
		},
	}
}

func fromModelSampleHistogramPair(modelSampleHistogram model.SampleHistogramPair, s *SampleHistogramPair) {
	buckets := make([]*HistogramBucket, len(modelSampleHistogram.Histogram.Buckets))

	for i, b := range modelSampleHistogram.Histogram.Buckets {
		buckets[i] = &HistogramBucket{
			Lower:      float64(b.Lower),
			Upper:      float64(b.Upper),
			Count:      float64(b.Count),
			Boundaries: int64(b.Boundaries),
		}
	}

	s.Timestamp = int64(modelSampleHistogram.Timestamp)
	s.Histogram = &SampleHistogram{
		Count:   float64(modelSampleHistogram.Histogram.Count),
		Sum:     float64(modelSampleHistogram.Histogram.Sum),
		Buckets: buckets,
	}
}
