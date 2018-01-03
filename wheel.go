package timerwheel

import (
	"errors"
	"sync"

	"github.com/coffeehc/logger"
)

type Wheel interface {
	tick() []string
	AddJob(job *Job) (run bool)
	RemoveJob(jobName string)
	GetMaxSlot(level int) uint64
	getNextSlotJobs() []string
	initSlot(slots []uint64) []string
}

func newWheel(slot Slot, parent Wheel, level int, jobService JobService) (Wheel, error) {
	if slot.GetMax() == 0 {
		return nil, errors.New("slot不能为0")
	}
	return &wheelImpl{
		parent:     parent,
		level:      level,
		jobs:       make([]string, 0),
		nextJobs:   make([]string, 0),
		mutex:      new(sync.Mutex),
		jobService: jobService,
		slot:       slot,
	}, nil
}

type wheelImpl struct {
	parent     Wheel
	level      int
	jobs       []string
	nextJobs   []string
	mutex      *sync.Mutex
	jobService JobService
	slot       Slot
}

func (impl *wheelImpl) initSlot(slots []uint64) []string {
	if impl.parent != nil {
		impl.jobs = impl.parent.initSlot(slots)
	}
	impl.slot.initSlot(slots[impl.level])
	jobs := make([]string, 0)
	for _, i := range impl.jobs {
		job := impl.jobService.Get(i)
		if job != nil {
			if impl.slot.Hit(job.slots[impl.level]) {
				jobs = append(jobs, i)
			}
		}
	}
	logger.Debug("wheel[%d] init slot[%d],current slot is %d,jobs is %#v", impl.level, impl.slot.GetMax(), impl.slot.CurrentSlot(), impl.jobs)
	return jobs
}

func (impl *wheelImpl) getNextSlotJobs() []string {
	impl.mutex.Lock()
	defer impl.mutex.Unlock()
	nextSlot, toZore := impl.slot.NextSlot()
	if toZore {
		if impl.parent != nil {
			impl.jobs = impl.parent.getNextSlotJobs()
		}
	}
	jobs := make([]string, 0)
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

func (impl *wheelImpl) tick() []string {
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
			impl.jobs = append(impl.jobs, job.Name)
		}
	} else {
		impl.jobs = append(impl.jobs, job.Name)
		run = true
	}
	if run {
		run = impl.slot.Hit(job.slots[impl.level])
		go func() {
			impl.nextJobs = impl.getNextSlotJobs()
		}()
	}
	if impl.parent == nil {
		logger.Debug("wheel add a job [%s]", job.Name)
	}
	return
}

func (impl *wheelImpl) RemoveJob(jobName string) {
	if impl.parent == nil {
		logger.Debug("wheel remove a job [%s]", jobName)
	}
	impl.mutex.Lock()
	var i = -1
	for j, _jobName := range impl.jobs {
		if _jobName == jobName {
			i = j
			break
		}
	}
	if i > -1 {
		impl.jobs = append(impl.jobs[:i], impl.jobs[i+1:]...)
	}
	impl.mutex.Unlock()
	if impl.parent != nil {
		impl.parent.RemoveJob(jobName)
	}
}
