package timerwheel

import (
	"errors"
	"sync"
)

type JobService interface {
	Get(jobId int64) *Job
	Remove(jobId int64)
	Add(job *Job) error
	AddOrUpdate(job *Job) (update bool)
}

func newJobService() JobService {
	return &jobServiceImpl{
		jobs: new(sync.Map),
	}
}

type jobServiceImpl struct {
	jobs *sync.Map
}

func (impl *jobServiceImpl) Get(jobId int64) *Job {
	v, ok := impl.jobs.Load(jobId)
	if ok {
		return v.(*Job)
	}
	return nil
}

func (impl *jobServiceImpl) Remove(jobId int64) {
	impl.jobs.Delete(jobId)
}

func (impl *jobServiceImpl) Add(job *Job) error {
	_, ok := impl.jobs.Load(job.Id)
	if ok {
		return errors.New("job已经存在")
	}
	impl.jobs.Store(job.Id, job)
	return nil
}
func (impl *jobServiceImpl) AddOrUpdate(job *Job) (update bool) {
	_, ok := impl.jobs.LoadOrStore(job.Id, job)
	if ok {
		impl.jobs.Store(job.Id, job)
		return ok
	}
	return ok
}
