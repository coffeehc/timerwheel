package timerwheel

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/coffeehc/logger"
)

type Config struct {
	Name       string
	WheelSlots []Slot
	Precision  time.Duration
	Location   *time.Location
	JobService JobService
}

type Service interface {
	Start()
	Stop()
	AddJob(job *Job) error
	RemoveJob(jobName string)
}

func NewService(config *Config) (Service, error) {
	if config.Location == nil {
		config.Location, _ = time.LoadLocation("Asia/Shanghai")
	}
	if config.Precision < time.Second {
		return nil, errors.New("时间精度必须大于等于一秒")
	}
	wheelCount := len(config.WheelSlots)
	if wheelCount == 0 {
		return nil, errors.New("没有定义任何一个时间轮")
	}
	if config.JobService == nil {
		config.JobService = newJobService()
	}
	var rootWheel Wheel = nil
	var err error
	for i := wheelCount - 1; i >= 0; i-- {
		rootWheel, err = newWheel(config.WheelSlots[i], rootWheel, i, config.JobService)
		if err != nil {
			return nil, err
		}
	}
	return &serviceImpl{
		name:       config.Name,
		precision:  config.Precision,
		rootWheel:  rootWheel,
		wheelCount: wheelCount,
		jobService: config.JobService,
		location:   config.Location,
		wheelSlots: config.WheelSlots,
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
	jobNames := impl.rootWheel.initSlot(slots)
	for _, jobName := range jobNames {
		go impl.execJob(impl.jobService.Get(jobName))
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
			jobs := impl.rootWheel.tick()
			for _, jobName := range jobs {
				go impl.execJob(impl.jobService.Get(jobName))
			}
		}
	}
}

func (impl *serviceImpl) RemoveJob(jobName string) {
	if impl.jobService.Get(jobName) != nil {
		impl.jobService.Remove(jobName)
		impl.rootWheel.RemoveJob(jobName)
	}
}

func (impl *serviceImpl) AddJob(job *Job) error {
	if impl.jobService.Get(job.Name) != nil {
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

func (impl *serviceImpl) execJob(job *Job) {
	if job == nil {
		return
	}
	defer func() {
		if err := recover(); err != nil {
			job.retryCount++
			logger.Error("job[%s]执行[%d次]失败:%#v", job.Name, job.retryCount, err)
			if job.retryCount < job.MaxRetry {
				go impl.execJob(job)
				return
			}
			job.retryCount = 0
		}
	}()
	retry, err := job.Handler(job.retryCount)
	if err == nil {
		job.retryCount = 0
		return
	}
	logger.Error("job[%s]执行[%d次]失败:%#v", job.Name, job.retryCount, err)
	job.retryCount++
	if retry && job.retryCount < job.MaxRetry {
		go impl.execJob(job)
		return
	}
	job.retryCount = 0

}
