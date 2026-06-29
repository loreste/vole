package server

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CronJob represents a scheduled task that runs a Vole command on a cron schedule.
type CronJob struct {
	Name      string
	Schedule  string
	Command   []string
	CreatedAt time.Time
	LastRun   time.Time
	RunCount  int64
	LastError string
	parsed    cronSchedule
}

type cronSchedule struct {
	minute []int // 0-59
	hour   []int // 0-23
	dom    []int // 1-31
	month  []int // 1-12
	dow    []int // 0-6 (Sunday=0)
}

// CronManager manages scheduled tasks that execute Vole commands.
type CronManager struct {
	mu   sync.RWMutex
	jobs map[string]*CronJob
}

// NewCronManager creates a new CronManager.
func NewCronManager() *CronManager {
	return &CronManager{jobs: make(map[string]*CronJob)}
}

// Add registers a new cron job with the given name, schedule, and command.
func (cm *CronManager) Add(name, schedule string, command []string) error {
	parsed, err := parseCronSchedule(schedule)
	if err != nil {
		return err
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.jobs[name] = &CronJob{
		Name:      name,
		Schedule:  schedule,
		Command:   command,
		CreatedAt: time.Now(),
		parsed:    parsed,
	}
	return nil
}

// Del removes a cron job by name. Returns true if the job existed.
func (cm *CronManager) Del(name string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, ok := cm.jobs[name]; !ok {
		return false
	}
	delete(cm.jobs, name)
	return true
}

// List returns all cron jobs sorted by name.
func (cm *CronManager) List() []*CronJob {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	jobs := make([]*CronJob, 0, len(cm.jobs))
	for _, j := range cm.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].Name < jobs[k].Name })
	return jobs
}

// Get returns a cron job by name.
func (cm *CronManager) Get(name string) (*CronJob, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	j, ok := cm.jobs[name]
	return j, ok
}

// Run checks all jobs and executes any that are due. Called every minute.
func (cm *CronManager) Run(now time.Time, exec func([]string) error) {
	cm.mu.Lock()
	var toRun []*CronJob
	for _, job := range cm.jobs {
		if matchesCron(job.parsed, now) {
			job.RunCount++
			job.LastRun = now
			toRun = append(toRun, job)
		}
	}
	cm.mu.Unlock()

	for _, job := range toRun {
		cmd := append([]string(nil), job.Command...)
		go func(j *CronJob) {
			if err := exec(cmd); err != nil {
				cm.mu.Lock()
				j.LastError = err.Error()
				cm.mu.Unlock()
				log.Printf("cron %q failed: %v", j.Name, err)
			}
		}(job)
	}
}

// StartLoop starts a background loop that checks and runs cron jobs every minute.
func (cm *CronManager) StartLoop(ctx context.Context, exec func([]string) error) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				cm.Run(t, exec)
			}
		}
	}()
}

func matchesCron(sched cronSchedule, t time.Time) bool {
	return contains(sched.minute, t.Minute()) &&
		contains(sched.hour, t.Hour()) &&
		contains(sched.dom, t.Day()) &&
		contains(sched.month, int(t.Month())) &&
		contains(sched.dow, int(t.Weekday()))
}

func contains(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// parseCronSchedule parses "minute hour dom month dow"
// Supports: *, */N, N, N-M, N,M,O
func parseCronSchedule(schedule string) (cronSchedule, error) {
	parts := strings.Fields(schedule)
	if len(parts) != 5 {
		return cronSchedule{}, fmt.Errorf("cron schedule must have 5 fields, got %d", len(parts))
	}
	minute, err := parseCronField(parts[0], 0, 59)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("minute: %v", err)
	}
	hour, err := parseCronField(parts[1], 0, 23)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("hour: %v", err)
	}
	dom, err := parseCronField(parts[2], 1, 31)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("day of month: %v", err)
	}
	month, err := parseCronField(parts[3], 1, 12)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("month: %v", err)
	}
	dow, err := parseCronField(parts[4], 0, 6)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("day of week: %v", err)
	}
	return cronSchedule{minute: minute, hour: hour, dom: dom, month: month, dow: dow}, nil
}

func parseCronField(field string, min, max int) ([]int, error) {
	if field == "*" {
		vals := make([]int, max-min+1)
		for i := range vals {
			vals[i] = min + i
		}
		return vals, nil
	}
	if strings.HasPrefix(field, "*/") {
		step, err := strconv.Atoi(field[2:])
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step %q", field)
		}
		var vals []int
		for i := min; i <= max; i += step {
			vals = append(vals, i)
		}
		return vals, nil
	}
	// Handle comma-separated and ranges
	var vals []int
	for _, part := range strings.Split(field, ",") {
		if idx := strings.IndexByte(part, '-'); idx >= 0 {
			lo, err := strconv.Atoi(part[:idx])
			if err != nil {
				return nil, err
			}
			hi, err := strconv.Atoi(part[idx+1:])
			if err != nil {
				return nil, err
			}
			for i := lo; i <= hi; i++ {
				vals = append(vals, i)
			}
		} else {
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, err
			}
			vals = append(vals, n)
		}
	}
	return vals, nil
}
