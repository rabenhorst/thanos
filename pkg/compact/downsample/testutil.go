package downsample

import (
	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"path/filepath"
	"testing"
)

func GetMetaAndChunks(t *testing.T, dir string, id ulid.ULID) (*metadata.Meta, []chunks.Meta) {
	newMeta, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
	testutil.Ok(t, err)

	//testutil.Equals(t, int64(400), newMeta.Thanos.Downsample.Resolution)

	indexr, err := index.NewFileReader(filepath.Join(dir, id.String(), block.IndexFilename))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, indexr.Close()) }()

	pall, err := indexr.Postings(index.AllPostingsKey())
	testutil.Ok(t, err)

	var series []storage.SeriesRef
	for pall.Next() {
		series = append(series, pall.At())
	}
	testutil.Ok(t, pall.Err())
	testutil.Equals(t, 10, len(series))

	var chks []chunks.Meta
	//var lset labels.Labels
	var builder labels.ScratchBuilder
	testutil.Ok(t, indexr.Series(series[0], &builder, &chks))
	//lset = builder.Labels()
	//testutil.Equals(t, labels.FromStrings("__name__", "a"), lset)

	return newMeta, chks
}

func GetAggregatorFromChunk(t *testing.T, chunkr *chunks.Reader, c chunks.Meta) (count []sample, sum []sample, counter []sample) {
	chk, err := chunkr.Chunk(c)
	testutil.Ok(t, err)

	ag, ok := chk.(*AggrChunk)
	testutil.Assert(t, ok)

	return expandHistogramAggregatorChunk(t, ag)
}

func expandHistogramAggregatorChunk(t *testing.T, c *AggrChunk) (count []sample, sum []sample, counter []sample) {
	countChunk, err := c.Get(AggrCount)
	testutil.Ok(t, err, "get histogram aggregator count chunk")
	it := countChunk.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		count = append(count, sample{t: t, v: v})
	}
	testutil.Ok(t, it.Err())

	sumChunk, err := c.Get(AggrSum)
	testutil.Ok(t, err, "get histogram aggregator sum chunk")
	it = sumChunk.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		t, fh := it.AtFloatHistogram()
		sum = append(sum, sample{t, 0, fh})
	}
	testutil.Ok(t, it.Err())

	counterChunk, err := c.Get(AggrCounter)
	testutil.Ok(t, err, "get histogram aggregator counter chunk")
	it = counterChunk.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		t, fh := it.AtFloatHistogram()
		counter = append(counter, sample{t, 0, fh})
	}
	testutil.Ok(t, it.Err())

	return count, sum, counter
}
