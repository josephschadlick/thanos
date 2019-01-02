package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/improbable-eng/thanos/pkg/objstore/inmem"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

func saveHeap(t *testing.T, name string) {
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	f, err := os.OpenFile("heap-"+name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	testutil.Ok(t, err)

	defer f.Close()
	testutil.Ok(t, pprof.WriteHeapProfile(f))
}

func TestBucketStore_PROFILE(t *testing.T) {
	bkt := inmem.NewBucket()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
	testutil.Ok(t, err)
	//defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	extLset := labels.FromStrings("ext1", "value1")

	start := time.Now()
	now := start

	var ids []ulid.ULID
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(dir, series[:4], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(dir, series[4:], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)

		ids = append(ids, id1, id2)
		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := block.ReadMetaFile(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		testutil.Ok(t, block.WriteMetaFile(log.NewNopLogger(), dir2, meta))

		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, dir1))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, dir2))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	store, err := NewBucketStore(nil, nil, bkt, dir, 100, 0, false)
	testutil.Ok(t, err)

	ctx, _ = context.WithTimeout(ctx, 30*time.Second)

	if err := runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if err := store.SyncBlocks(ctx); err != nil {
			return err
		}
		if store.numBlocks() < 6 {
			return errors.New("not all blocks loaded")
		}
		return nil
	}); err != nil && errors.Cause(err) != context.Canceled {
		t.Error(err)
		t.FailNow()
	}
	testutil.Ok(t, err)

	pbseries := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
	}

	srv := newStoreSeriesServer(ctx)

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

	g := sync.WaitGroup{}

	// NO REPRO
	go func() {
		g.Add(1)
		time.Sleep(10 * time.Millisecond)
		// Simulate deleted blocks without sync (compaction!)
		testutil.Ok(t, block.Delete(ctx, bkt, ids[2]))
		time.Sleep(10 * time.Millisecond)
		store.SyncBlocks(ctx)
		store.SyncBlocks(ctx)

		g.Done()
	}()

	for i := 0; i < 1000; i++ {
		go func() {
			g.Add(1)
			srv := newStoreSeriesServer(ctx)

			err = store.Series(&storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime: timestamp.FromTime(start),
				MaxTime: timestamp.FromTime(now),
			}, srv)
			fmt.Println(err)
			//testutil.Ok(t, err)
			//testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

			g.Done()
		}()
	}
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 1000; i++ {
		go func() {
			g.Add(1)
			srv := newStoreSeriesServer(ctx)

			err = store.Series(&storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime: timestamp.FromTime(start),
				MaxTime: timestamp.FromTime(now),
			}, srv)
			fmt.Println(err)
			//testutil.Ok(t, err)
			//testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

			g.Done()
		}()
	}

	g.Wait()

	//for i, s := range srv.SeriesSet {
	//	testutil.Equals(t, pbseries[i], s.Labels)
	//	testutil.Equals(t, 3, len(s.Chunks))
	//}

	saveHeap(t, "2")
}

/*
==================
WARNING: DATA RACE
Read at 0x00c4201c22f8 by goroutine 75:
  github.com/improbable-eng/thanos/pkg/pool.(*BytesPool).Put()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/pool/pool.go:83 +0x14c
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).Close()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1570 +0x115
  github.com/improbable-eng/thanos/pkg/runutil.CloseWithLogOnErr()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/runutil/runutil.go:60 +0x59
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).Series()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:811 +0x2d7e
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func2()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:140 +0x4e6

Previous write at 0x00c4201c22f8 by goroutine 25:
  sync/atomic.AddInt64()
      /usr/local/go/src/runtime/race_amd64.s:276 +0xb
  github.com/improbable-eng/thanos/pkg/pool.(*BytesPool).Get()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/pool/pool.go:65 +0x1ad
  github.com/improbable-eng/thanos/pkg/store.(*bucketBlock).readChunkRange()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1130 +0x95
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).loadChunks()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1499 +0xe7
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload.func3()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1485 +0x23a
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34

Goroutine 75 (running) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:136 +0x238e
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d

Goroutine 25 (finished) created at:
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:37 +0x10b
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1493 +0x6f0
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).blockSeries()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:572 +0x1109
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).Series.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:721 +0x1e7
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34
==================
<nil>
==================
WARNING: DATA RACE
Write at 0x00c42029c2fc by goroutine 10:
  internal/race.Write()
      /usr/local/go/src/internal/race/race.go:41 +0x38
  sync.(*WaitGroup).Wait()
      /usr/local/go/src/sync/waitgroup.go:127 +0xf3
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:164 +0x2442
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d

Previous read at 0x00c42029c2fc by goroutine 74:
  internal/race.Read()
      /usr/local/go/src/internal/race/race.go:37 +0x38
  sync.(*WaitGroup).Add()
      /usr/local/go/src/sync/waitgroup.go:70 +0x16e
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func2()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:137 +0x5c

Goroutine 10 (running) created at:
  testing.(*T).Run()
      /usr/local/go/src/testing/testing.go:824 +0x564
  testing.runTests.func1()
      /usr/local/go/src/testing/testing.go:1063 +0xa4
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d
  testing.runTests()
      /usr/local/go/src/testing/testing.go:1061 +0x4e1
  testing.(*M).Run()
      /usr/local/go/src/testing/testing.go:978 +0x2cd
  main.main()
      _testmain.go:70 +0x22a

Goroutine 74 (running) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:136 +0x238e
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d
==================
==================
WARNING: DATA RACE
Write at 0x00c4202647b0 by goroutine 230:
  runtime.mapdelete_faststr()
      /usr/local/go/src/runtime/hashmap_fast.go:883 +0x0
  github.com/improbable-eng/thanos/pkg/objstore/inmem.(*Bucket).Delete()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/objstore/inmem/inmem.go:138 +0x69
  github.com/improbable-eng/thanos/pkg/objstore.DeleteDir.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/objstore/objstore.go:102 +0x113
  github.com/improbable-eng/thanos/pkg/objstore/inmem.(*Bucket).Iter()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/objstore/inmem/inmem.go:76 +0x616
  github.com/improbable-eng/thanos/pkg/objstore.DeleteDir()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/objstore/objstore.go:97 +0x10c
  github.com/improbable-eng/thanos/pkg/block.Delete()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/block/block.go:215 +0x7f
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func3()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:157 +0xae

Previous read at 0x00c4202647b0 by goroutine 85:
  runtime.mapaccess2_faststr()
      /usr/local/go/src/runtime/hashmap_fast.go:261 +0x0
  github.com/improbable-eng/thanos/pkg/objstore/inmem.(*Bucket).GetRange()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/objstore/inmem/inmem.go:103 +0x9b
  github.com/improbable-eng/thanos/pkg/store.(*bucketBlock).readChunkRange()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1136 +0x255
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).loadChunks()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1499 +0xe7
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload.func3()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1485 +0x23a
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34

Goroutine 230 (running) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:154 +0x2431
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d

Goroutine 85 (finished) created at:
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:37 +0x10b
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1493 +0x6f0
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).blockSeries()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:572 +0x1109
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).Series.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:721 +0x1e7
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34
==================
==================
WARNING: DATA RACE
Read at 0x00c4200d4978 by goroutine 76:
  github.com/improbable-eng/thanos/pkg/pool.(*BytesPool).Put()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/pool/pool.go:83 +0x14c
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).Close()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1570 +0x115
  github.com/improbable-eng/thanos/pkg/runutil.CloseWithLogOnErr()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/runutil/runutil.go:60 +0x59
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).Series()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:811 +0x2d7e
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func2()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:140 +0x4e6

Previous write at 0x00c4200d4978 by goroutine 365:
  sync/atomic.AddInt64()
      /usr/local/go/src/runtime/race_amd64.s:276 +0xb
  github.com/improbable-eng/thanos/pkg/pool.(*BytesPool).Get()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/pool/pool.go:65 +0x1ad
  github.com/improbable-eng/thanos/pkg/store.(*bucketBlock).readChunkRange()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1130 +0x95
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).loadChunks()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1499 +0xe7
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload.func3()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1485 +0x23a
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34

Goroutine 76 (running) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:136 +0x238e
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d

Goroutine 365 (finished) created at:
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:37 +0x10b
  github.com/improbable-eng/thanos/pkg/store.(*bucketChunkReader).preload()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:1493 +0x6f0
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).blockSeries()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:572 +0x1109
  github.com/improbable-eng/thanos/pkg/store.(*BucketStore).Series.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket.go:721 +0x1e7
  github.com/improbable-eng/thanos/vendor/github.com/oklog/run.(*Group).Run.func1()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/vendor/github.com/oklog/run/group.go:38 +0x34
==================
<nil>
==================
WARNING: DATA RACE
Write at 0x00c4200837d0 by goroutine 77:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func2()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:140 +0x50b

Previous write at 0x00c4200837d0 by goroutine 76:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE.func2()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:140 +0x50b

Goroutine 77 (running) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:136 +0x238e
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d

Goroutine 76 (finished) created at:
  github.com/improbable-eng/thanos/pkg/store.TestBucketStore_PROFILE()
      /home/bartek/Repos/thanosGo/src/github.com/improbable-eng/thanos/pkg/store/bucket_profile_test.go:136 +0x238e
  testing.tRunner()
      /usr/local/go/src/testing/testing.go:777 +0x16d
==================

*/
