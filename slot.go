package timerwheel

import "time"

type Slot interface {
	GetMax() uint64
	CurrentSlot() uint64
	CheckHit(slot, slotProfile uint64) bool
	Hit(profile uint64) bool
	initSlot(slot uint64)
	Tick() (toZore bool)
	NextSlot() (slot uint64, toZore bool)
}

func NewSlot(max uint64) Slot {
	return &slotImpl{
		currentSlot: 0,
		maxSlot:     max,
	}
}

type slotImpl struct {
	currentSlot uint64
	maxSlot     uint64
}

func (impl *slotImpl) CheckHit(slot, slotProfile uint64) bool {
	return 1<<slot&slotProfile != 0
}

func (impl *slotImpl) Hit(slotProfile uint64) bool {
	return impl.CheckHit(impl.currentSlot, slotProfile)
}

func (impl *slotImpl) GetMax() uint64 {
	return impl.maxSlot
}
func (impl *slotImpl) CurrentSlot() uint64 {
	return impl.currentSlot
}
func (impl *slotImpl) initSlot(slot uint64) {
	impl.currentSlot = slot
}
func (impl *slotImpl) Tick() (toZore bool) {
	impl.currentSlot, toZore = impl.NextSlot()
	return toZore
}

func (impl *slotImpl) NextSlot() (slot uint64, toZore bool) {
	slot = impl.currentSlot + 1
	if slot >= impl.maxSlot {
		return 0, true
	}
	return slot, false
}

func NewWeekSlot() Slot {
	return &weekSlotImpl{
		slotImpl{maxSlot: 7, currentSlot: 0},
	}
}

type weekSlotImpl struct {
	slotImpl
}

func (impl *weekSlotImpl) initSlot(slot uint64) {
	impl.currentSlot = (slot + 4) % impl.maxSlot
}

func NewMonthSlot(location *time.Location) Slot {
	if location == nil {
		location = defaultLocatioin
	}
	return &monthSlotImpl{
		slotImpl: slotImpl{maxSlot: 31, currentSlot: 1},
		location: location,
	}
}

type monthSlotImpl struct {
	slotImpl
	location *time.Location
}

func (impl *monthSlotImpl) NextSlot() (uint64, bool) {
	slot := uint64(time.Now().In(impl.location).Add(time.Hour * 24).Day())
	return slot, slot == 1
}

func (impl *monthSlotImpl) initSlot(slot uint64) {
	impl.currentSlot = uint64(time.Now().In(impl.location).Day())
}
