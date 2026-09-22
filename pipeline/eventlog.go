package pipeline

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 磁盘帧布局（大端）：
//
//	magic(2)=0x4556 | version(1)=1 | flags(1) | offset(8) |
//	storedAtUnixNano(8) | payloadLen(4) | payload(payloadLen) | crc32IEEE(4)
//
// CRC 覆盖整帧 CRC 字段之前的所有字节。末尾残缺帧在打开时截断；
// 中间任何帧 magic/长度/CRC/JSON 不合法均视为 ErrLogCorrupt。
const (
	frameMagic0  byte = 0x45
	frameMagic1  byte = 0x56
	frameVersion byte = 1

	frameHeaderLen = 2 + 1 + 1 + 8 + 8 + 4 // magic,ver,flags,offset,storedAt,len
	frameCRCLen    = 4
)

type frameHeader struct {
	offset   int64
	storedAt time.Time
	payloadN int
}

// EventLog 是本地 append-only 事件日志（单文件）。
type EventLog struct {
	path string

	mu      sync.RWMutex
	f       *os.File
	starts  []int64 // starts[offset] = 该帧在文件中的起始字节
	next    int64
	subs    map[int]chan struct{}
	nextSub int
	closed  bool
}

// OpenEventLog 打开（不存在则创建）本地事件日志并完成崩溃恢复扫描。
func OpenEventLog(path string) (*EventLog, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("pipeline: create log dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("pipeline: open event log: %w", err)
	}
	l := &EventLog{
		path: path,
		f:    f,
		subs: make(map[int]chan struct{}),
	}
	if err := l.recoverScan(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// recoverScan 顺序校验每一帧；末尾残缺截断，中间损坏报错。
// 一旦发生截断，截断后重新整体扫描并立即以其结果为准返回。
func (l *EventLog) recoverScan() error {
	starts, next, pos, err := l.scanValidate(nil, 0, 0, false)
	if err != errTornTail {
		if err != nil {
			return err
		}
		if err := l.f.Sync(); err != nil {
			return fmt.Errorf("pipeline: sync event log: %w", err)
		}
		l.starts = starts
		l.next = next
		return nil
	}

	// 末尾撕裂帧：截断到 pos 并 fsync，然后重新完整扫描。
	if err := l.f.Truncate(pos); err != nil {
		return fmt.Errorf("pipeline: truncate torn tail: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("pipeline: sync after tail truncate: %w", err)
	}
	starts, next, _, err = l.scanValidate(nil, 0, 0, true)
	if err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("pipeline: sync event log: %w", err)
	}
	l.starts = starts
	l.next = next
	return nil
}

// errTornTail 是内部标记：读到了文件末尾的残缺帧（撕裂写）。
var errTornTail = fmt.Errorf("torn tail frame")

// scanValidate 从 pos 开始顺序校验帧；返回累计的 starts、下一偏移与停止位置。
func (l *EventLog) scanValidate(starts []int64, next, pos int64, afterTruncate bool) ([]int64, int64, int64, error) {
	fi, err := l.f.Stat()
	if err != nil {
		return nil, 0, pos, fmt.Errorf("pipeline: stat event log: %w", err)
	}
	size := fi.Size()
	buf := make([]byte, 64*1024)
	for pos < size {
		frameStart := pos
		hdr, err := readHeaderAt(l.f, pos, buf)
		if err != nil {
			if !afterTruncate && (err == io.ErrUnexpectedEOF || err == io.EOF) {
				return starts, next, frameStart, errTornTail
			}
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: %v", ErrLogCorrupt, pos, err)
		}
		end := pos + int64(frameHeaderLen+hdr.payloadN+frameCRCLen)
		if end > size {
			if !afterTruncate {
				return starts, next, frameStart, errTornTail
			}
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: partial frame after truncate", ErrLogCorrupt, pos)
		}
		if hdr.offset != next {
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: gap/disorder frame offset=%d want=%d",
				ErrLogCorrupt, pos, hdr.offset, next)
		}
		frame, err := readFullAt(l.f, pos, int(end-pos), buf)
		if err != nil {
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: %v", ErrLogCorrupt, pos, err)
		}
		got := binary.BigEndian.Uint32(frame[len(frame)-4:])
		if want := crc32.ChecksumIEEE(frame[:len(frame)-4]); got != want {
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: crc mismatch", ErrLogCorrupt, pos)
		}
		var anyJSON any
		if err := json.Unmarshal(frame[frameHeaderLen:frameHeaderLen+hdr.payloadN], &anyJSON); err != nil {
			return nil, 0, frameStart, fmt.Errorf("%w at byte %d: payload not json: %v", ErrLogCorrupt, pos, err)
		}
		starts = append(starts, frameStart)
		pos = end
		next++
	}
	return starts, next, pos, nil
}

func readHeaderAt(f *os.File, pos int64, buf []byte) (frameHeader, error) {
	hbytes, err := readFullAt(f, pos, frameHeaderLen, buf)
	if err != nil {
		return frameHeader{}, err
	}
	if hbytes[0] != frameMagic0 || hbytes[1] != frameMagic1 {
		return frameHeader{}, fmt.Errorf("bad magic %x%x", hbytes[0], hbytes[1])
	}
	if hbytes[2] != frameVersion {
		return frameHeader{}, fmt.Errorf("unsupported version %d", hbytes[2])
	}
	off := int64(binary.BigEndian.Uint64(hbytes[4:12]))
	ts := int64(binary.BigEndian.Uint64(hbytes[12:20]))
	n := int(binary.BigEndian.Uint32(hbytes[20:24]))
	return frameHeader{
		offset:   off,
		storedAt: time.Unix(0, ts),
		payloadN: n,
	}, nil
}

// readFullAt 必须使用绝对偏移读取：os.File 会被并发调用复用，直接对 f 做
// io.ReadFull 会沿用文件当前偏移（且 O_APPEND 下读行为易被误用），因此
// 每次都基于 SectionReader(f, pos) 从 pos 处精确读满 n 字节。
func readFullAt(f *os.File, pos int64, n int, buf []byte) ([]byte, error) {
	var out []byte
	if n <= len(buf) {
		out = buf[:n]
	} else {
		out = make([]byte, n)
	}
	if _, err := io.ReadFull(io.NewSectionReader(f, pos, int64(n)), out); err != nil {
		return nil, err
	}
	return out, nil
}

// Path 返回日志文件路径。
func (l *EventLog) Path() string { return l.path }

// Earliest 返回日志中仍保留的最早偏移（当前实现不做截断/压缩，恒为 0）。
func (l *EventLog) Earliest() int64 { return 0 }

// NextOffset 返回下一条事件将占用的偏移（即当前事件条数）。
func (l *EventLog) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.next
}

// Append 把事件原子追加到日志末尾（单次 write + fsync），返回其偏移。
func (l *EventLog) Append(ev Event) (int64, error) {
	if ev.ID == "" {
		return 0, ErrEventIDEmpty
	}
	if ev.Key == "" {
		return 0, ErrEventKeyEmpty
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	storedAt := time.Now()

	payload, err := json.Marshal(ev)
	if err != nil {
		return 0, fmt.Errorf("pipeline: marshal event: %w", err)
	}
	if len(payload) > 1<<31 {
		return 0, fmt.Errorf("pipeline: event payload too large: %d", len(payload))
	}

	frame := make([]byte, frameHeaderLen+len(payload)+frameCRCLen)
	frame[0] = frameMagic0
	frame[1] = frameMagic1
	frame[2] = frameVersion
	frame[3] = 0
	binary.BigEndian.PutUint64(frame[4:12], 0) // offset 先占位
	binary.BigEndian.PutUint64(frame[12:20], uint64(storedAt.UnixNano()))
	binary.BigEndian.PutUint32(frame[20:24], uint32(len(payload)))
	copy(frame[frameHeaderLen:], payload)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrPipelineClosed
	}
	offset := l.next
	binary.BigEndian.PutUint64(frame[4:12], uint64(offset))
	// CRC 必须在所有字段（含 offset）填写完毕后最后计算，否则重启扫描会
	// 因 CRC 覆盖到零值占位而误判日志损坏。
	binary.BigEndian.PutUint32(frame[len(frame)-4:], crc32.ChecksumIEEE(frame[:len(frame)-4]))

	// 写前文件大小即本帧起始字节；O_APPEND 保证帧总是尾接，持锁保证无并发写。
	fi, err := l.f.Stat()
	if err != nil {
		return 0, fmt.Errorf("pipeline: stat before append: %w", err)
	}
	frameStart := fi.Size()
	if _, err := l.f.Write(frame); err != nil {
		return 0, fmt.Errorf("pipeline: append event: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return 0, fmt.Errorf("pipeline: fsync event: %w", err)
	}
	l.starts = append(l.starts, frameStart)
	l.next++
	l.wakeSubs()
	return offset, nil
}

// ReadAt 读取指定偏移处的事件。
func (l *EventLog) ReadAt(offset int64) (StoredEvent, error) {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return StoredEvent{}, ErrPipelineClosed
	}
	if offset < 0 || offset >= l.next {
		l.mu.RUnlock()
		return StoredEvent{}, fmt.Errorf("%w: read offset=%d next=%d", ErrOffsetBeyondEnd, offset, l.next)
	}
	start := l.starts[offset]
	l.mu.RUnlock()

	hdrBuf := make([]byte, frameHeaderLen)
	if _, err := io.ReadFull(io.NewSectionReader(l.f, start, frameHeaderLen), hdrBuf); err != nil {
		return StoredEvent{}, fmt.Errorf("pipeline: read header offset=%d: %w", offset, err)
	}
	n := int(binary.BigEndian.Uint32(hdrBuf[20:24]))
	body := make([]byte, n+frameCRCLen)
	if _, err := io.ReadFull(io.NewSectionReader(l.f, start+frameHeaderLen, int64(len(body))), body); err != nil {
		return StoredEvent{}, fmt.Errorf("pipeline: read body offset=%d: %w", offset, err)
	}
	payload := body[:n]
	got := binary.BigEndian.Uint32(body[n:])
	if want := crc32.ChecksumIEEE(append(hdrBuf, payload...)); got != want {
		return StoredEvent{}, fmt.Errorf("%w: crc mismatch offset=%d", ErrLogCorrupt, offset)
	}
	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		return StoredEvent{}, fmt.Errorf("%w: bad event json offset=%d: %v", ErrLogCorrupt, offset, err)
	}
	ts := int64(binary.BigEndian.Uint64(hdrBuf[12:20]))
	return StoredEvent{
		Offset:   offset,
		Event:    ev,
		StoredAt: time.Unix(0, ts),
	}, nil
}

// Stream 从 from（含）开始向 ch 顺序投递事件：先回放历史，再实时推送后续
// Append，直到 ctx 取消。函数返回（含 ctx 取消）时总会关闭 ch。
//
// 不丢失保证：Stream 直接以日志为唯一数据源，订阅通道仅作“有新数据”的
// 唤醒信号（信号丢失也无妨），事件本身绝不依赖内存通道传递。
func (l *EventLog) Stream(ctx context.Context, from int64, ch chan<- StoredEvent, poll time.Duration) error {
	defer close(ch)
	if from < 0 {
		return fmt.Errorf("%w: stream from=%d", ErrOffsetBeforeEarliest, from)
	}
	wake, cancel := l.subscribe()
	defer cancel()

	pos := from
	timer := time.NewTimer(poll)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := l.NextOffset()
		for ; pos < end; end = l.NextOffset() {
			se, err := l.ReadAt(pos)
			if err != nil {
				return err
			}
			select {
			case ch <- se:
				pos++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		timer.Reset(poll)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-timer.C:
		}
	}
}

func (l *EventLog) subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	l.mu.Lock()
	id := l.nextSub
	l.nextSub++
	l.subs[id] = ch
	l.mu.Unlock()
	return ch, func() {
		l.mu.Lock()
		delete(l.subs, id)
		l.mu.Unlock()
	}
}

// wakeSubs 非阻塞地唤醒所有流（仅作信号，丢信号由轮询兜底）。
func (l *EventLog) wakeSubs() {
	for _, ch := range l.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Close 释放日志资源。
func (l *EventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.f.Close()
}
