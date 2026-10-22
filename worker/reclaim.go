package worker

// processingReclaimer is implemented by stores that keep a processing list per
// queue (Redis) and can move abandoned jobs back onto the queue.
type processingReclaimer interface {
	ReclaimProcessing(queueName string) (int, error)
}

// reclaimProcessing moves jobs left in the processing list by a crashed process
// back onto the queue. Stores without a processing list are skipped. A failure is
// logged and does not stop the workers from starting.
func (w *Worker) reclaimProcessing() {
	reclaimer, ok := w.store.(processingReclaimer)
	if !ok {
		return
	}
	moved, err := reclaimer.ReclaimProcessing(w.queueName)
	if err != nil {
		w.logger.Error("Failed to reclaim processing jobs", "queue", w.queueName, "error", err)
		return
	}
	if moved > 0 {
		w.logger.Info("Reclaimed processing jobs", "queue", w.queueName, "count", moved)
	}
}
