package timerwheel

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/coffeehc/logger"
)

type JobHandler func(retryCount int64) (retry bool, err error)

type Job struct {
	Id         int64
	Handler    JobHandler
	MaxRetry   int64
	Slots      []string //当slot为0的时候,表示在任何一个槽位都执行,槽位号从1开始
	slots      []uint64
	retryCount int64
}

type Service interface {
	Start()
	Stop()
	AddJob(job *Job) error
	RemoveJob(jobId int64)
}

func NewService(name string, wheelSlots []Slot, precision time.Duration, location *time.Location) (Service, error) {
	if location == nil {
		location, _ = time.LoadLocation("Asia/Shanghai")
	}
	if precision < time.Second {
		return nil, errors.New("时间精度必须大于等于一秒")
	}
	wheelCount := len(wheelSlots)
	if wheelCount == 0 {
		return nil, errors.New("没有定义任何一个时间轮")
	}
	var rootWheel Wheel = nil
	var err error
	jobService := newJobService()
	for i := wheelCount - 1; i >= 0; i-- {
		rootWheel, err = newWheel(wheelSlots[i], rootWheel, i, jobService)
		if err != nil {
			return nil, err
		}
	}
	return &serviceImpl{
		name:       name,
		precision:  precision,
		rootWheel:  rootWheel,
		wheelCount: wheelCount,
		jobService: jobService,
		location:   location,
		wheelSlots: wheelSlots,
	}, nil
}

type serviceImpl struct {
	name       string
	precision  time.Duration
	ticker     *time.Ticker
	stop       chan struct{}
	running    int64
	rootWheel  Wheel
	wheelCount int
	jobService JobService
	location   *time.Location
	wheelSlots []Slot
}

func (impl *serviceImpl) Stop() {
	atomic.StoreInt64(&impl.running, 0)
	close(impl.stop)
	logger.Info("停止时间轮[%s]服务", impl.name)
}

func (impl *serviceImpl) Start() {
	impl.stop = make(chan struct{}, 1)
	locZoreTime, _ := time.ParseInLocation("2006-01-02", time.Unix(0, 0).Format("2006-01-02"), impl.location)
	offset := uint64(time.Since(locZoreTime))
	slots := make([]uint64, impl.wheelCount)
	ss := offset / uint64(impl.precision)
	prevSlot := uint64(1)
	for i, slot := range impl.wheelSlots {
		slots[i] = (ss % (prevSlot * slot.GetMax())) / prevSlot
		prevSlot *= slot.GetMax()
	}
	logger.Debug("初始化slot为:%#v", slots)
	jobIds := impl.rootWheel.initSlot(slots)
	for _, jobId := range jobIds {
		go impl.execJob(jobId)
	}
	logger.Info("启动时间轮[%s]服务", impl.name)
	go impl.startTicker()
}

func (impl *serviceImpl) startTicker() {
	if !atomic.CompareAndSwapInt64(&impl.running, 0, 1) {
		return
	}
	impl.ticker = time.NewTicker(impl.precision)
	for {
		select {
		case <-impl.stop:
			impl.ticker.Stop()
			logger.Info("停止时间轮服务")
			return
		case <-impl.ticker.C:
			jobIds := impl.rootWheel.tick()
			for _, jobId := range jobIds {
				go impl.execJob(jobId)
			}
		}
	}
}

func (impl *serviceImpl) RemoveJob(jobId int64) {
	if impl.jobService.Get(jobId) != nil {
		impl.jobService.Remove(jobId)
		impl.rootWheel.RemoveJob(jobId)
	}
}

func (impl *serviceImpl) AddJob(job *Job) error {
	if impl.jobService.Get(job.Id) != nil {
		return errors.New("该job已经存在")
	}
	if len(job.Slots) != impl.wheelCount {
		return errors.New("指定的slot个数与轮数不符")
	}
	job.slots = make([]uint64, impl.wheelCount)
	for i := 0; i < impl.wheelCount; i++ {
		maxSlot := impl.rootWheel.GetMaxSlot(i)
		if maxSlot == 0 {
			return errors.New("无法获取最大的槽位数")
		}
		slot, err := ParseCronField(job.Slots[i], maxSlot)
		if err != nil {
			return err
		}
		job.slots[i] = slot
	}
	impl.jobService.Add(job)
	impl.rootWheel.AddJob(job)
	return nil
}

func (impl *serviceImpl) execJob(jobId int64) {
	job := impl.jobService.Get(jobId)
	if job == nil {

		return
	}
	defer func() {
		if err := recover(); err != nil {
			job.retryCount++
			if job.retryCount < job.MaxRetry {
				go impl.execJob(jobId)
			}
			logger.Error("执行job出错:%#v", err)
		}
	}()
	retry, err := job.Handler(job.retryCount)
	if err == nil {
		return
	}
	logger.Error("job[%d]执行失败:%#v", job.Id, err)
	job.retryCount++
	if retry && job.retryCount < job.MaxRetry {
		go impl.execJob(jobId)
	}
	job.retryCount = 0
}
