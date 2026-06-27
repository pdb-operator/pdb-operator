/*
Copyright 2025 The PDB Operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"strconv"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
)

// maxMaintenanceRequeue bounds the proactive requeue so the controller still wakes
// at least this often to re-evaluate an upcoming window.
const maxMaintenanceRequeue = time.Hour

// maintenanceWindowSpan is a normalized, timezone-resolved maintenance window.
type maintenanceWindowSpan struct {
	loc   *time.Location
	start time.Duration         // offset from local midnight
	end   time.Duration         // offset from local midnight; < start means the window crosses midnight
	days  map[time.Weekday]bool // empty means every day
}

// collectMaintenanceWindows normalizes the annotation window and any policy windows
// into evaluable spans. Unparseable windows are skipped, matching prior best-effort behavior.
func collectMaintenanceWindows(config *AvailabilityConfig) []maintenanceWindowSpan {
	if config == nil {
		return nil
	}
	var spans []maintenanceWindowSpan
	if s, ok := parseAnnotationWindow(config.MaintenanceWindow); ok {
		spans = append(spans, s)
	}
	for i := range config.MaintenanceWindows {
		if s, ok := parsePolicyWindow(config.MaintenanceWindows[i]); ok {
			spans = append(spans, s)
		}
	}
	return spans
}

// parseAnnotationWindow parses the annotation form "HH:MM-HH:MM [Timezone]" (every day).
func parseAnnotationWindow(window string) (maintenanceWindowSpan, bool) {
	if window == "" {
		return maintenanceWindowSpan{}, false
	}
	parts := strings.Fields(window)
	timeRange := parts[0]
	timezone := "UTC"
	if len(parts) > 1 {
		timezone = parts[1]
	}
	bounds := strings.Split(timeRange, "-")
	if len(bounds) != 2 {
		return maintenanceWindowSpan{}, false
	}
	return buildSpan(bounds[0], bounds[1], timezone, nil)
}

// parsePolicyWindow parses a structured PDBPolicy maintenance window, honoring DaysOfWeek.
func parsePolicyWindow(w availabilityv1alpha1.MaintenanceWindow) (maintenanceWindowSpan, bool) {
	timezone := w.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	var days map[time.Weekday]bool
	if len(w.DaysOfWeek) > 0 {
		days = make(map[time.Weekday]bool, len(w.DaysOfWeek))
		for _, d := range w.DaysOfWeek {
			if d >= 0 && d <= 6 {
				days[time.Weekday(d)] = true
			}
		}
	}
	return buildSpan(w.Start, w.End, timezone, days)
}

func buildSpan(start, end, timezone string, days map[time.Weekday]bool) (maintenanceWindowSpan, bool) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return maintenanceWindowSpan{}, false
	}
	startOff, ok := parseHHMM(start)
	if !ok {
		return maintenanceWindowSpan{}, false
	}
	endOff, ok := parseHHMM(end)
	if !ok {
		return maintenanceWindowSpan{}, false
	}
	return maintenanceWindowSpan{loc: loc, start: startOff, end: endOff, days: days}, true
}

func parseHHMM(s string) (time.Duration, bool) {
	hm := strings.Split(s, ":")
	if len(hm) != 2 {
		return 0, false
	}
	h, err := strconv.Atoi(hm[0])
	if err != nil || h < 0 || h > 23 {
		return 0, false
	}
	m, err := strconv.Atoi(hm[1])
	if err != nil || m < 0 || m > 59 {
		return 0, false
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, true
}

func (s maintenanceWindowSpan) dayAllowed(wd time.Weekday) bool {
	if len(s.days) == 0 {
		return true
	}
	return s.days[wd]
}

// active reports whether now falls inside this window.
func (s maintenanceWindowSpan) active(now time.Time) bool {
	local := now.In(s.loc)
	tod := time.Duration(local.Hour())*time.Hour + time.Duration(local.Minute())*time.Minute
	if s.start <= s.end {
		return s.dayAllowed(local.Weekday()) && tod >= s.start && tod < s.end
	}
	// Window crosses midnight: the post-midnight tail belongs to the previous day's window.
	if tod >= s.start {
		return s.dayAllowed(local.Weekday())
	}
	if tod < s.end {
		return s.dayAllowed(prevWeekday(local.Weekday()))
	}
	return false
}

// nextStart returns the duration from now until this window next opens.
func (s maintenanceWindowSpan) nextStart(now time.Time) (time.Duration, bool) {
	local := now.In(s.loc)
	midnight := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, s.loc)
	for i := 0; i <= 7; i++ {
		day := midnight.AddDate(0, 0, i)
		if !s.dayAllowed(day.Weekday()) {
			continue
		}
		start := day.Add(s.start)
		if start.After(now) {
			return start.Sub(now), true
		}
	}
	return 0, false
}

func prevWeekday(wd time.Weekday) time.Weekday {
	return time.Weekday((int(wd) + 6) % 7)
}

// IsInMaintenanceWindow reports whether now is within any window configured on config
// (annotation window or policy windows).
func IsInMaintenanceWindow(config *AvailabilityConfig, now time.Time) bool {
	for _, s := range collectMaintenanceWindows(config) {
		if s.active(now) {
			return true
		}
	}
	return false
}

// durationUntilMaintenanceWindow returns the time until the soonest window opens, and
// whether any window is configured at all.
func durationUntilMaintenanceWindow(config *AvailabilityConfig, now time.Time) (time.Duration, bool) {
	var best time.Duration
	found := false
	for _, s := range collectMaintenanceWindows(config) {
		if d, ok := s.nextStart(now); ok && (!found || d < best) {
			best, found = d, true
		}
	}
	return best, found
}

// applyMaintenanceRequeue ensures result wakes the controller at (or before) the next
// window start, so an idle workload still has its PDB relaxed when the window opens.
func applyMaintenanceRequeue(result ctrl.Result, config *AvailabilityConfig, now time.Time) ctrl.Result {
	until, ok := durationUntilMaintenanceWindow(config, now)
	if !ok {
		return result
	}
	if until > maxMaintenanceRequeue {
		until = maxMaintenanceRequeue
	}
	if result.RequeueAfter == 0 || until < result.RequeueAfter {
		result.RequeueAfter = until
	}
	return result
}
