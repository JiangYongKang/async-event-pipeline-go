package pipeline

import "context"

// crashProcess 模拟进程被杀死：取消运行时（在途重试立即放弃，不等待终态），
// 随后关闭文件句柄。已 fsync 的日志与快照保持完好，未终态事件下次重启重放。
func crashProcess(p *Pipeline) {
	if p.rt != nil {
		p.rt.streamCancel()
		p.rt.runCancel()
		// 不等待 jobsWG：模拟进程立即退出。
		p.rt = nil
	}
	_ = p.log.Close()
	_ = p.store.Close()
}

// 仅供 crash 测试编译期引用 context（helper 保持与包内测试同包）。
var _ = context.Background
