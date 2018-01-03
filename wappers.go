package timerwheel

import "time"

var defaultLocatioin, _ = time.LoadLocation("Asia/Shanghai")

//日时间轮,slot为: 60(秒),60(分),24(小时) ,如每天1点开始执行写为: 0 0 1
//适用于24小时内的循环任务执行,如,每5分钟执行一次 59 */5 *
func NewDayTimerWheel(name string) Service {
	service, _ := NewService(&Config{
		Name:       name,
		WheelSlots: []Slot{NewSlot(60), NewSlot(60), NewSlot(24)},
		Precision:  time.Second,
		Location:   defaultLocatioin,
	})
	return service
}

//日时间轮,slot为: 60(秒),60(分),24(小时),7(周) ,如每天1点开始执行写为: 0 0 1
//适用于24小时内的循环任务执行,如,每5分钟执行一次 59 */5 *
func NewWeekTimerWheel(name string) Service {
	service, _ := NewService(&Config{
		Name:       name,
		WheelSlots: []Slot{NewSlot(60), NewSlot(60), NewSlot(24), NewWeekSlot()},
		Precision:  time.Second,
		Location:   defaultLocatioin,
	})
	return service
}

//日时间轮,slot为: 60(秒),60(分),24(小时),7(周) ,如每天1点开始执行写为: 0 0 1
//适用于24小时内的循环任务执行,如,每5分钟执行一次 59 */5 *
func NewMonthTimerWheel(name string) Service {
	service, _ := NewService(&Config{
		Name:       name,
		WheelSlots: []Slot{NewSlot(60), NewSlot(60), NewSlot(24), NewWeekSlot(), NewMonthSlot(defaultLocatioin)},
		Precision:  time.Second,
		Location:   defaultLocatioin,
	})
	return service
}
