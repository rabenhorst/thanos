package downsamplev2

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/promql-engine/engine"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
)

const (
	maxSamplesPerChunk = 120
)

var (
	promAggregators = map[downsample.AggrType]string{
		downsample.AggrCount:   "count_over_time",
		downsample.AggrSum:     "sum",
		downsample.AggrMin:     "min",
		downsample.AggrMax:     "max",
		downsample.AggrCounter: "increase",
	}
)

type blockQuerier struct {
	q storage.Querier
}

func (b *blockQuerier) Querier(context.Context, int64, int64) (storage.Querier, error) {
	return b.q, nil
}

func Downsample(
	ctx context.Context,
	logger log.Logger,
	origMeta *metadata.Meta,
	b tsdb.BlockReader,
	dir string,
	resolution int64,
) (id ulid.ULID, err error) {
	if origMeta.Thanos.Downsample.Resolution >= resolution {
		return id, errors.New("target resolution not lower than existing one")
	}

	indexr, err := b.Index()
	if err != nil {
		return id, errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "downsample index reader")

	// Generate new block id.
	uid := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))

	// Create block directory to populate with chunks, meta and index files into.
	blockDir := filepath.Join(dir, uid.String())
	if err := os.MkdirAll(blockDir, 0750); err != nil {
		return id, errors.Wrap(err, "mkdir block dir")
	}

	// Remove blockDir in case of errors.
	defer func() {
		if err != nil {
			var merr errutil.MultiError
			merr.Add(err)
			merr.Add(os.RemoveAll(blockDir))
			err = merr.Err()
		}
	}()

	// Copy original meta to the new one. Update downsampling resolution and ULID for a new block.
	newMeta := *origMeta
	newMeta.Thanos.Downsample.Resolution = resolution
	newMeta.ULID = uid

	// Writes downsampled chunks right into the files, avoiding excess memory allocation.
	// Flushes index and meta data after aggregations.
	streamedBlockWriter, err := downsample.NewStreamedBlockWriter(blockDir, indexr, logger, newMeta)
	if err != nil {
		return id, errors.Wrap(err, "get streamed block writer")
	}
	defer runutil.CloseWithErrCapture(&err, streamedBlockWriter, "close stream block writer")

	// Create new promql engine.
	opts := promql.EngineOpts{
		Logger:        logger,
		Reg:           nil,
		MaxSamples:    math.MaxInt32,
		Timeout:       30 * time.Second,
		LookbackDelta: time.Hour,
	}
	ng := engine.New(engine.Opts{EngineOpts: opts})

	// Downsample raw block.
	if origMeta.Thanos.Downsample.Resolution == 0 {
		q, err := tsdb.NewBlockQuerier(b, b.Meta().MinTime, b.Meta().MaxTime)
		if err != nil {
			return id, errors.Wrap(err, "create querier")
		}

		// Get all series from the block.
		seriesSet := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

		for seriesSet.Next() {
			lset := seriesSet.At().Labels()
			chks, err := aggrChks(ctx, &blockQuerier{q: q}, ng, lset, time.UnixMilli(b.Meta().MinTime), time.UnixMilli(b.Meta().MaxTime), 5*time.Minute)
			if err != nil {
				return id, errors.Wrapf(err, "downsample series: %v", lset.String())
			}
			if err := streamedBlockWriter.WriteSeries(lset, chks); err != nil {
				return id, errors.Wrapf(err, "write series: %v", lset.String())
			}
		}
	} else {
		q, err := aggrChunksBlockQueryable(ctx, logger, dir, origMeta)
		if err != nil {
			return id, errors.Wrap(err, "create queryable for aggregated block")
		}

		qq, err := q.Querier(ctx, b.Meta().MinTime, b.Meta().MaxTime)
		if err != nil {
			return id, errors.Wrap(err, "create querier for aggregated block")
		}

		seriesSet := qq.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

		for seriesSet.Next() {
			lset := seriesSet.At().Labels()
			chks, err := aggrChks(ctx, q, ng, lset, time.UnixMilli(b.Meta().MinTime), time.UnixMilli(b.Meta().MaxTime), time.Hour)
			if err != nil {
				return id, errors.Wrapf(err, "downsample series: %v", lset.String())
			}
			if err := streamedBlockWriter.WriteSeries(lset, chks); err != nil {
				return id, errors.Wrapf(err, "write series: %v", lset.String())
			}
		}

	}
	return uid, nil
}

func aggrChks(
	ctx context.Context,
	q storage.Queryable,
	ng v1.QueryEngine,
	lset labels.Labels,
	bMint, bMaxt time.Time,
	resolution time.Duration,
) ([]chunks.Meta, error) {
	var (
		aggrSeries [5]*promql.Series
		err        error
	)

	for _, aggrType := range []downsample.AggrType{downsample.AggrCount, downsample.AggrSum, downsample.AggrMin, downsample.AggrMax, downsample.AggrCounter} {
		aggrSeries[aggrType], err = queryAggr(ctx, ng, q, lset, aggrType, bMint, bMaxt, resolution)
		if err != nil {
			return nil, err
		}
		// TODO: query first value of counter
		if aggrType == downsample.AggrCounter {
			aggrSeries[aggrType].Floats = append([]promql.FPoint{{T: 0, F: 0}}, aggrSeries[aggrType].Floats...)
		}
	}

	if !aligned(aggrSeries) {
		return nil, errors.New("series aggregates are not aligned")
	}

	var aggrChunks []chunks.Meta

	counter := 0
	ab := newAggrChunkBuilder()
	for i := range aggrSeries[downsample.AggrCounter].Floats {
		if counter >= maxSamplesPerChunk {
			aggrChunks = append(aggrChunks, ab.encode())
			ab = newAggrChunkBuilder()
			counter = 0
		}
		ab.add(int64(i), aggrSeries)
		counter++
	}
	aggrChunks = append(aggrChunks, ab.encode())

	return aggrChunks, nil
}

func queryAggr(ctx context.Context, ng v1.QueryEngine, q storage.Queryable, lset labels.Labels, aggrType downsample.AggrType, mint, maxt time.Time, step time.Duration) (*promql.Series, error) {
	sq, err := ng.NewRangeQuery(ctx, q, &promql.QueryOpts{}, queryString(aggrType, step, lset), mint, maxt, step)
	if err != nil {
		return nil, err
	}
	sqres := sq.Exec(ctx)
	if sqres.Err != nil {
		return nil, sqres.Err
	}
	var res promql.Series
	switch sqres.Value.Type() {
	case parser.ValueTypeMatrix:
		matrix := sqres.Value.(promql.Matrix)
		if len(matrix) > 1 {
			return nil, errors.New("more than one series returned")
		}
		res = matrix[0]
	default:
		return nil, errors.New("unknown result type")
	}
	return &res, nil
}

type aggrChunkBuilder struct {
	mint, maxt int64

	chunks [5]chunkenc.Chunk
	apps   [5]chunkenc.Appender
}

func queryString(aggrType downsample.AggrType, vectorRange time.Duration, series labels.Labels) string {
	builder := strings.Builder{}
	builder.WriteString(promAggregators[aggrType])
	builder.WriteString("({")
	for i, l := range series {
		builder.WriteString(fmt.Sprintf("%s=\"%s\"", l.Name, l.Value))
		if i != len(series)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("}")
	if aggrType == downsample.AggrCounter || aggrType == downsample.AggrCount {
		_, _ = fmt.Fprintf(&builder, "[%s]", vectorRange)
	}
	builder.WriteString(")")
	return builder.String()
}

func newAggrChunkBuilder() *aggrChunkBuilder {
	b := &aggrChunkBuilder{
		mint: math.MaxInt64,
		maxt: math.MinInt64,
	}
	b.chunks[downsample.AggrCount] = chunkenc.NewXORChunk()
	b.chunks[downsample.AggrSum] = chunkenc.NewXORChunk()
	b.chunks[downsample.AggrMin] = chunkenc.NewXORChunk()
	b.chunks[downsample.AggrMax] = chunkenc.NewXORChunk()
	b.chunks[downsample.AggrCounter] = chunkenc.NewXORChunk()

	for i, c := range b.chunks {
		if c != nil {
			b.apps[i], _ = c.Appender()
		}
	}
	return b
}

func (b *aggrChunkBuilder) add(i int64, aggrSeries [5]*promql.Series) {
	t := aggrSeries[downsample.AggrCounter].Floats[i].T
	if t < b.mint {
		b.mint = t
	}
	if t > b.maxt {
		b.maxt = t
	}
	for j := 0; j <= int(downsample.AggrCounter); j++ {
		// Panic here means misaligned series.
		b.apps[j].Append(aggrSeries[j].Floats[i].T, aggrSeries[j].Floats[i].F)
	}
}

func (b *aggrChunkBuilder) encode() chunks.Meta {
	return chunks.Meta{
		MinTime: b.mint,
		MaxTime: b.maxt,
		Chunk:   downsample.EncodeAggrChunk(b.chunks),
	}
}

// Checks if all series have the same length and time range.
func aligned(series [5]*promql.Series) bool {
	for _, s := range series {
		if len(s.Floats) != len(series[0].Floats) {
			return false
		}
		if s.Floats[0].T != series[0].Floats[0].T {
			return false
		}
		if s.Floats[len(s.Floats)-1].T != series[0].Floats[len(series[0].Floats)-1].T {
			return false
		}
	}
	return true
}

// aggrChunksBlockQueryable creates a queryable for an aggregated chunks block in the give directory.
// TODO: Replaced by a queryable that can be directly created from an aggregated chunks block.
func aggrChunksBlockQueryable(ctx context.Context, logger log.Logger, dir string, meta *metadata.Meta) (storage.Queryable, error) {
	bkt, err := filesystem.NewBucket(dir)
	if err != nil {
		return nil, err
	}

	bs, err := store.NewBucketStore(
		objstore.WithNoopInstr(bkt),
		nil,
		"",
		store.NewChunksLimiterFactory(10000/store.MaxSamplesPerChunk),
		store.NewSeriesLimiterFactory(0),
		store.NewBytesLimiterFactory(0),
		store.NewGapBasedPartitioner(store.PartitionerMaxGapSize),
		10,
		false,
		store.DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
	)
	if err != nil {
		return nil, err
	}

	if err := bs.AddBlock(ctx, meta); err != nil {
		return nil, err
	}

	return query.NewQueryableCreator(logger, nil, bs, 2, 30*time.Second)(
		false,
		nil,
		nil,
		9999999,
		false,
		false,
		false,
		nil,
		query.NoopSeriesStatsReporter,
	), nil
}
