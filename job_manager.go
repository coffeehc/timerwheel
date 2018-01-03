package timerwheel

import (
	"errors"
	"sync"
)

type JobHandler func(retryCount int64) (retry bool, err error)

type Job struct {
	Name       string
	Handler    JobHandler
	MaxRetry   int64
	Slots      []string //当slot为0的时候,表示在任何一个槽位都执行,槽位号从1开始
	slots      []uint64
	retryCount int64
}

type JobService interface {
	Get(jobName string) *Job
	Remove(jobName string)
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

func (impl *jobServiceImpl) Get(jobName string) *Job {
	v, ok := impl.jobs.Load(jobName)
	if ok {
		return v.(*Job)
	}
	return nil
}

func (impl *jobServiceImpl) Remove(jobName string) {
	impl.jobs.Delete(jobName)
}

func (impl *jobServiceImpl) Add(job *Job) error {
	_, ok := impl.jobs.Load(job.Name)
	if ok {
		return errors.New("job已经存在")
	}
	if job.MaxRetry == 0 {
		job.MaxRetry = 3
	}
	impl.jobs.Store(job.Name, job)
	return nil
}
func (impl *jobServiceImpl) AddOrUpdate(job *Job) (update bool) {
	_, ok := impl.jobs.LoadOrStore(job.Name, job)
	if ok {
		impl.jobs.Store(job.Name, job)
		return ok
	}
	return ok
}
