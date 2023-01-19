package queryrange

import (
	"fmt"
	"github.com/prometheus/common/model"
)

// Following two functions are slightly modified and copied from
// https://github.com/prometheus/common/blob/846591a166358c7048ef197e84501ca688dda920/model/value_histogram.go#L141-L163

func (s SampleHistogramPair) MarshalJSON() ([]byte, error) {
	t, err := json.Marshal(s.Timestamp)
	if err != nil {
		return nil, err
	}
	v, err := json.Marshal(s.Histogram)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[%s,%s]", t, v)), nil
}

func (s *SampleHistogramPair) UnmarshalJSON(buf []byte) error {
	var t model.Time
	tmp := []interface{}{&t, &s.Histogram}
	wantLen := len(tmp)
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	if gotLen := len(tmp); gotLen != wantLen {
		return fmt.Errorf("wrong number of fields: %d != %d", gotLen, wantLen)
	}
	s.Timestamp = int64(t)
	return nil
}

func (s SampleHistogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Count   model.FloatString  `json:"count"`
		Sum     model.FloatString  `json:"sum"`
		Buckets []*HistogramBucket `json:"buckets"`
	}{
		Count:   model.FloatString(s.Count),
		Sum:     model.FloatString(s.Sum),
		Buckets: s.Buckets,
	})
}

func (s *SampleHistogram) UnmarshalJSON(buf []byte) error {
	var sampleHistogram struct {
		Count   model.FloatString  `json:"count"`
		Sum     model.FloatString  `json:"sum"`
		Buckets []*HistogramBucket `json:"buckets"`
	}
	if err := json.Unmarshal(buf, &sampleHistogram); err != nil {
		return err
	}
	s.Count = float64(sampleHistogram.Count)
	s.Sum = float64(sampleHistogram.Sum)
	s.Buckets = sampleHistogram.Buckets
	return nil
}

func (s HistogramBucket) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(s.Boundaries)
	if err != nil {
		return nil, err
	}
	l, err := json.Marshal(s.Lower)
	if err != nil {
		return nil, err
	}
	u, err := json.Marshal(s.Upper)
	if err != nil {
		return nil, err
	}
	c, err := json.Marshal(s.Count)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[%s,\"%s\",\"%s\",\"%s\"]", b, l, u, c)), nil
}

func (s *HistogramBucket) UnmarshalJSON(buf []byte) error {
	var lower model.FloatString
	var upper model.FloatString
	var count model.FloatString
	tmp := []interface{}{&s.Boundaries, &lower, &upper, &count}
	wantLen := len(tmp)
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	if gotLen := len(tmp); gotLen != wantLen {
		return fmt.Errorf("wrong number of fields: %d != %d", gotLen, wantLen)
	}

	s.Lower = float64(lower)
	s.Upper = float64(upper)
	s.Count = float64(count)

	return nil
}
