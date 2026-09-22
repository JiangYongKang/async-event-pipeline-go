package pipeline_test

import (
	"errors"
	"os"
	"testing"

	"github.com/highcumontoa/async-event-pipeline-go/pipeline"
)

// 撕裂写恢复：模拟进程崩溃于一次帧写入中途，末尾出现半帧。
// 重新打开必须截断半帧并保留之前完好的事件，而不是报损坏或静默丢失。
func TestEventLogTornWriteTruncation(t *testing.T) {
	path := t.TempDir() + "/events.log"
	l, err := pipeline.OpenEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		off, err := l.Append(pipeline.Event{
			ID:      "evt",
			Key:     "k",
			Type:    "inc",
			Payload: []byte(string(rune('a' + i))),
		})
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset=%d want %d", off, i)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	fi, _ := os.Stat(path)
	full := fi.Size()

	// 1) 追加 17 个随机垃圾字节（撕裂帧）。
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	garbage := []byte{0x45, 0x56, 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0x7f}
	if _, err := f.Write(garbage); err != nil {
		t.Fatal(err)
	}
	_ = f.Sync()
	_ = f.Close()

	l2, err := pipeline.OpenEventLog(path)
	if err != nil {
		t.Fatalf("torn tail should be truncated, got: %v", err)
	}
	if got := l2.NextOffset(); got != 5 {
		t.Fatalf("after torn-tail recovery next=%d want 5", got)
	}
	if fi2, _ := os.Stat(path); fi2.Size() != full {
		t.Fatalf("file size=%d want %d (torn bytes removed)", fi2.Size(), full)
	}
	se, err := l2.ReadAt(4)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ASSERT torn-write: recovered 5 events, last id=%s offset=%d size=%d",
		se.Event.ID, se.Offset, full)
	_ = l2.Close()
}

// 中间帧 CRC 被破坏必须返回 ErrLogCorrupt，不得静默跳过或截断。
func TestEventLogMidCorruptionRejected(t *testing.T) {
	path := t.TempDir() + "/events.log"
	l, _ := pipeline.OpenEventLog(path)
	_, _ = l.Append(pipeline.Event{ID: "e0", Key: "k", Type: "inc", Payload: []byte("a")})
	_, _ = l.Append(pipeline.Event{ID: "e1", Key: "k", Type: "inc", Payload: []byte("b")})
	_ = l.Close()

	// 翻转第一帧 payload 中的一个字节（payload 起始于 24 字节头部之后）。
	data, _ := os.ReadFile(path)
	data[30] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.OpenEventLog(path); !errors.Is(err, pipeline.ErrLogCorrupt) {
		t.Fatalf("mid corruption err=%v want ErrLogCorrupt", err)
	}
	t.Logf("ASSERT mid-corruption: reopening returns ErrLogCorrupt (no silent skip, no data loss)")
}
