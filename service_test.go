package timerwheel_test

import (
	"testing"
	"time"

	"github.com/coffeehc/logger"
	"github.com/coffeehc/timerwheel"
)

func TestGetField(t *testing.T) {
	cron := "0-10/3"
	maxSlot := uint64(63)
	i, err := timerwheel.ParseCronField(cron, maxSlot)
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}
	t.Log(i)
	for j := uint64(0); j <= maxSlot; j++ {
		t.Log("%d->%t", j, 1<<j&i == 0)
	}
}

func TestNewService(t *testing.T) {
	logger.SetDefaultLevel("/", logger.LevelDebug)
	var location, _ = time.LoadLocation("Asia/Shanghai")
	service, err := timerwheel.NewService(&timerwheel.Config{
		Name: "test",
		WheelSlots: []timerwheel.Slot{
			timerwheel.NewSlot(60),
			timerwheel.NewSlot(60),
			timerwheel.NewSlot(24),
			timerwheel.NewWeekSlot(),
			timerwheel.NewMonthSlot(location),
		},
		Precision: time.Second,
		Location:  location})
	if err != nil {
		t.Errorf("err is %#v", err)
		t.FailNow()
	}
	err = service.AddJob(&timerwheel.Job{
		Name:    "job-1",
		Handler: buildJobHander(1),
		Slots:   []string{"2,5,8,10,15,30,40,50,55", "*", "*", "*", "*"},
	})
	if err != nil {
		t.Errorf("err is %#v", err)
		t.FailNow()
	}
	service.Start()
	time.Sleep(time.Minute)
	service.RemoveJob("job-1")
	time.Sleep(time.Minute)
}

func buildJobHander(id int64) timerwheel.JobHandler {
	return func(retryCount int64) (retry bool, err error) {
		logger.Debug("do job %d,%s", id, time.Now())
		return false, nil
	}
}
