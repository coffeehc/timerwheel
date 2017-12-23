package timerwheel

import (
	"errors"
	"sync"

	"github.com/coffeehc/logger"
)

type Wheel interface {
	tick() []int64
	AddJob(job *Job) (run bool)
	RemoveJob(jobId int64)
	GetMaxSlot(level int) uint64
	getNextSlotJobs() []int64
	initSlot(slots []uint64) []int64
}

func newWheel(slot Slot, parent Wheel, level int, jobService JobService) (Wheel, error) {
	if slot.GetMax() == 0 {
		return nil, errors.New("slot不能为0")
	}
	return &wheelImpl{
		parent:     parent,
		level:      level,
		jobs:       make([]int64, 0),
		nextJobs:   make([]int64, 0),
		mutex:      new(sync.Mutex),
		jobService: jobService,
		slot:       slot,
	}, nil
}

type wheelImpl struct {
	parent     Wheel
	level      int
	jobs       []int64
	nextJobs   []int64
	mutex      *sync.Mutex
	jobService JobService
	slot       Slot
}

func (impl *wheelImpl) initSlot(slots []uint64) []int64 {
	if impl.parent != nil {
		impl.jobs = impl.parent.initSlot(slots)
	}
	impl.slot.initSlot(slots[impl.level])
	jobs := make([]int64, 0)
	for _, i := range impl.jobs {
		job := impl.jobService.Get(i)
		if job != nil {
			if impl.slot.Hit(job.slots[impl.level]) {
				jobs = append(jobs, i)
			}
		}
	}
	logger.Debug("wheel[%d] init slot,current slot is %d,jobs is %#v", impl.level, impl.slot.CurrentSlot(), impl.jobs)
	return jobs
}

func (impl *wheelImpl) getNextSlotJobs() []int64 {
	impl.mutex.Lock()
	defer impl.mutex.Unlock()
	nextSlot, toZore := impl.slot.NextSlot()
	if toZore {
		if impl.parent != nil {
			impl.jobs = impl.parent.getNextSlotJobs()
		}
	}
	jobs := make([]int64, 0)
	for _, i := range impl.jobs {
		job := impl.jobService.Get(i)
		if job != nil {
			if impl.slot.CheckHit(nextSlot, job.slots[impl.level]) {
				jobs = append(jobs, i)
			}
		}
	}
	return jobs
}

func (impl *wheelImpl) GetMaxSlot(level int) uint64 {
	if level == impl.level {
		return impl.slot.GetMax()
	}
	if impl.parent == nil {
		return 0
	}
	return impl.parent.GetMaxSlot(level)
}

func (impl *wheelImpl) tick() []int64 {
	impl.mutex.Lock()
	jobs := impl.nextJobs
	impl.mutex.Unlock()
	if impl.slot.Tick() {
		if impl.parent != nil {
			impl.parent.tick()
		}
		//TODO 此处可以加一个tick扩展,用于在日期的处理上大月小月的时候,让父级轮多跳一次的需求
	}
	go func() {
		impl.nextJobs = impl.getNextSlotJobs()
	}()
	//logger.Debug("wheel[%d] tick,current _slot is %d",impl.level,impl._slot)
	return jobs
}

func (impl *wheelImpl) AddJob(job *Job) (run bool) {
	if impl.parent != nil {
		impl.mutex.Lock()
		run = impl.parent.AddJob(job)
		impl.mutex.Unlock()
		if run {
			impl.jobs = append(impl.jobs, job.Id)
		}
	} else {
		impl.jobs = append(impl.jobs, job.Id)
		run = true
	}
	if run {
		run = impl.slot.Hit(job.slots[impl.level])
		go func() {
			impl.nextJobs = impl.getNextSlotJobs()
		}()
	}
	logger.Debug("wheel[%d] add a job [%d],in current _slot[%d] run ? [%t]", impl.level, job.Id, impl.slot.CurrentSlot(), run)
	logger.Debug("wheel[%d] jobs is [%q]", impl.level, impl.jobs)
	return
}

func (impl *wheelImpl) RemoveJob(jobId int64) {
	logger.Debug("wheel[%d] remove a job [%d]", impl.level, jobId)
	impl.mutex.Lock()
	var i = -1
	for j, _jobId := range impl.jobs {
		if _jobId == jobId {
			i = j
			break
		}
	}
	if i > -1 {
		impl.jobs = append(impl.jobs[:i], impl.jobs[i+1:]...)
	}
	impl.mutex.Unlock()
	if impl.parent != nil {
		impl.parent.RemoveJob(jobId)
	}
}
