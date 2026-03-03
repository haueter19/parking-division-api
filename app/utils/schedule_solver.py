from collections import defaultdict, Counter
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
from app import db
from ortools.sat.python import cp_model

class ParkingScheduler:
    def __init__(self, week_start, db):
        self.week_start = week_start
        self.days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        self.weekdays = {'Mon', 'Tue', 'Wed', 'Thu', 'Fri'}
        self.shifts = None
        self.shift_ids = None
        self.garages = None
        self.employees = None
        self.requests = None
        self.availability = None
        self.SCALE = 100
        self.model = cp_model.CpModel()
        self.solver = cp_model.CpSolver()
        self._assign = {}
        self._solution = None
        self.db = db
        self.access_db = r'F:\Pkcommon\1-SHARED FILES\TOR databases\TOR 2026.accdb'
        self.conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={self.access_db};Pwd=VacaMSU;'  # Replace with the actual path
        )
        self.conn = pyodbc.connect(self.conn_str)
    
    def build(self):
        self._setup_variables()
        #self.diagnose()
        self._apply_hard_constraints()
        self._apply_availability()
        self._apply_fixed_schedules()
        self._apply_soft_constraints()
        return self  # allows chaining: scheduler.build().solve()

    def solve(self, time_limit=30.0):
        self.solver.parameters.max_time_in_seconds = time_limit
        status = self.solver.Solve(self.model)
        self._solution = status

        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            print(f"Status: {'OPTIMAL' if status == cp_model.OPTIMAL else 'FEASIBLE'}")
        
            schedule = defaultdict(list)
            for s in self.shift_ids:
                garage, booth, day, start, end = self.shifts[s]
                for c in self.all_cashiers:
                    if self.solver.Value(self._assign[(c, s)]) == 1:
                        schedule[day].append((garage, booth, start, end, c))
        
            for day in self.days:
                if day in schedule:
                    print(f"--- {day} ---")
                    for garage, booth, start, end, cashier in sorted(schedule[day]):
                        print(f"  {garage:10s} {booth}  {start:5.2f}-{end:5.2f}  -> {cashier}")
                    print()
        
            # Summary: which garages does each cashier cover?
            print("--- Cashier garage assignments ---")
            for c in self.all_cashiers:
                garages_worked = [g for g in self.garages if self.solver.Value(self.works_at_garage[(c, g)]) == 1]
                if garages_worked:
                    print(f"  {c}: {', '.join(garages_worked)}")

            self.schedule = schedule

        else:
            print("No solution found.")
        
        return status

    def _setup_variables(self):
        self.get_shifts()
        self.shift_ids = list(range(len(self.shifts)))
        self.garages = list(dict.fromkeys(s[0] for s in self.shifts))  # unique, order preserved
        self.get_employees('viewer')       # DataFrame from your DB query
        #HOURLY_CASHIERS = self.employees[self.employees['Type']=='Hourly']['cashier_id'].tolist()
        #PERMANENT_CASHIERS = self.employees[self.employees['Type']=='Full Time']['cashier_id'].tolist()
        #self.all_cashiers = PERMANENT_CASHIERS + HOURLY_CASHIERS
        self.all_cashiers = self.employees['employee_id'].tolist()
        self.get_requests()

    
    def _apply_hard_constraints(self):
        """
        """
        # Decision variables
        # assign[c][s] = 1 if cashier c works shift s
        self._assign = {}
        for c in self.all_cashiers:
            for s in self.shift_ids:
                self._assign[(c, s)] = self.model.NewBoolVar(f'self._assign_{c}_{s}')

        
        # Hard constraint: each shift filled by exactly 1 cashier
        for s in self.shift_ids:
            self.model.AddExactlyOne(self._assign[(c, s)] for c in self.all_cashiers)

        # Hard constraint: no cashier works two overlapping shifts
        # Two shifts overlap if they share a day AND their time windows intersect
        for c in self.all_cashiers:
            for s1 in self.shift_ids:
                for s2 in self.shift_ids:
                    if s2 <= s1:
                        continue
                    if self.shifts_overlap(s1, s2):
                        self.model.Add(self._assign[(c, s1)] + self._assign[(c, s2)] <= 1)

        # Hard constraint: max 40 paid hours per week
        for c in self.all_cashiers:
            self.model.Add(
                sum(self._assign[(c, s)] * int(self.paid_hours(s) * self.SCALE) for s in self.shift_ids)
                <= 40 * self.SCALE
            )
        
        # Hard constraint: Treleven can only work shifts <= 6 hours
        treleven_id = int(
            self.employees[self.employees['last_name'] == 'Treleven'].iloc[0]['employee_id']
        )
        for s in self.shift_ids:
            _, _, _, start, end = self.shifts[s]
            if (end - start) > 6.0:
                self.model.Add(self._assign[(treleven_id, s)] == 0)
        
        # Hard constraint: Wood only available M-F at 7pm or later, full availability Sat/Sun
        wood_id = int(self.employees[self.employees['last_name']=='Wood'].iloc[0]['employee_id'])
        for s in self.shift_ids:
            _, _, day, start, end = self.shifts[s]
            if day in self.weekdays and start < 19.0:
                self.model.Add(self._assign[(wood_id, s)] == 0)
        
        # Hard constraint: Siegel only available M-F at 5:15pm or later, full Sat/Sun
        siegel_id = int(self.employees[self.employees['last_name']=='Siegel'].iloc[0]['employee_id'])
        for s in self.shift_ids:
            _, _, day, start, end = self.shifts[s]
            if day in self.weekdays and start < 17.25:
                self.model.Add(self._assign[(siegel_id, s)] == 0)
        return
        

    def _apply_soft_constraints(self):
        """
        """
        # Soft constraint: prefer cashier works at only 1 garage per week
        # Penalty added to objective for each extra garage a cashier works at
        works_at_garage = {}
        for c in self.all_cashiers:
            for g in self.garages:
                garage_shifts = [s for s in self.shift_ids if self.shifts[s][0] == g]
                works_at_garage[(c, g)] = self.model.NewBoolVar(f'works_{c}_{g}')
                # works_at_garage is 1 if any shift at this garage is assigned
                self.model.AddMaxEquality(
                    works_at_garage[(c, g)],
                    [self._assign[(c, s)] for s in garage_shifts]
                )
        
        # Soft constraint (stronger): penalize each cashier who works at 2+ garages
        SINGLE_LOCATION_PENALTY = 1000  # high weight relative to other objective terms
        
        penalty_terms = []
        
        for c in self.all_cashiers:
            for g1 in self.garages:
                for g2 in self.garages:
                    if g2 <= g1:
                        continue
                    # both_garages = 1 if cashier c works at BOTH g1 and g2
                    both_garages = self.model.NewBoolVar(f'both_{c}_{g1}_{g2}')
                    self.model.AddMinEquality(
                        both_garages,
                        [works_at_garage[(c, g1)], works_at_garage[(c, g2)]]
                    )
                    penalty_terms.append(both_garages * SINGLE_LOCATION_PENALTY)
        
        self.works_at_garage = works_at_garage
        
        # Soft constraint: Punwar prefers not to work Sat/Sun
        WEEKEND_PENALTY = 500
        
        punwar_weekend_terms = []
        for s in self.shift_ids:
            _, _, day, _, _ = self.shifts[s]
            if day in {'Sat', 'Sun'}:
                punwar_weekend_terms.append(self._assign[(self.employees[self.employees['last_name']=='Punwar'].iloc[0]['employee_id'], s)] * WEEKEND_PENALTY)
        
        # Soft constraint: Punwar prefers <= 15 hours per week
        # Penalize each hour over 15
        PUNWAR_HOURS_PENALTY = 300  # per 0.01 hour unit over 15, scaled
        
        punwar_hours = self.model.NewIntVar(0, 40 * self.SCALE, 'punwar_hours')
        self.model.Add(
            punwar_hours == sum(self._assign[(self.employees[self.employees['last_name']=='Punwar'].iloc[0]['employee_id'], s)] * int(self.paid_hours(s) * self.SCALE) for s in self.shift_ids)
        )
        
        punwar_overage = self.model.NewIntVar(0, 40 * self.SCALE, 'punwar_overage')
        self.model.Add(punwar_overage >= punwar_hours - int(15 * self.SCALE))
        self.model.Add(punwar_overage >= 0)

        # Combined objective
        self.model.Minimize(
            sum(penalty_terms)            # single-location soft constraint from before
            + sum(punwar_weekend_terms)   # Punwar weekend penalty
            + punwar_overage * PUNWAR_HOURS_PENALTY  # Punwar hours penalty
        )
        return
        
        
    def _apply_fixed_schedules(self):
        """
        """
        # Hard constraint: McConley/Chan fixed schedule
        # Find matching shift indices and force assign to McConley/Chan
        mcconley_assignments = {
            ('Frances', '2', 'Tue'): 14,
            ('Frances', '2', 'Wed'): 14,
            ('Frances', '2', 'Thu'): 14,
            ('Frances', '2', 'Fri'): 14,
            ('Frances', '2', 'Sat'): 14,
        }        
        chan_assignments = {
            ('Frances', '1', 'Mon'): 10,
            ('Frances', '1', 'Tue'): 10,
            ('Frances', '1', 'Wed'): 10,
            ('Frances', '1', 'Thu'): 10,
            ('Frances', '1', 'Fri'): 10,
        }
        
        fixed_assignments = {**mcconley_assignments, **chan_assignments}
        
        for s in self.shift_ids:
            garage, booth, day, start, end = self.shifts[s]
            key = (garage, booth, day)
            if key in fixed_assignments:
                cashier = fixed_assignments[key]
                self.model.Add(self._assign[(cashier, s)] == 1)
                # Block everyone else from this shift
                for c in self.all_cashiers:
                    if c != cashier:
                        self.model.Add(self._assign[(c, s)] == 0)
    
                        
    def _apply_availability(self):
        """
        """
        if not isinstance(self.requests, pd.DataFrame):
            self.get_requests()
        
        availability = {}
        for employee_number in self.employees:
            if employee_number not in availability.keys():
                availability[employee_number] = []
        
            days_requested = self.requests[self.requests['employee_id']==employee_number]['day_of_week'].tolist()
            days_available = []
            for day in self.days:
                if day not in days_requested:
                    days_available.append(day)
        
            availability[employee_number] = set(days_available)

        self.availability = availability
        
        for c, available_days in self.availability.items():
            for s in self.shift_ids:
                _, _, day, _, _ = self.shifts[s]
                if day not in available_days:
                    self.model.Add(self._assign[(c, s)] == 0)
                    
        return availability

    def get_shifts(self):
        # Returns a list of tuples suitable for JSON serialization
        shifts_df = pd.read_sql("""
            SELECT * FROM PUReporting.app.schedule_shifts WHERE week_start_date = ?
            """, self.db.bind, params=(self.week_start,))

        shifts = []
        for _, row in shifts_df[['location', 'booth', 'day_of_week', 'start_hour', 'end_hour']].iterrows():
            shifts.append(tuple(row))

        self.shifts = shifts
        return shifts

    def get_employees(self, role):
        """
        """
        employees = pd.read_sql(f"""
            SELECT 
                e.employee_id, first_name, last_name, e.role, c.cashier_id
            FROM pt.employees e
            INNER JOIN app.cashier_id c On (e.employee_id=c.employee_id)
            WHERE e.role = '{role}'
            """, self.db.bind)
        
        self.employees = employees
        
        return employees
        
    def get_requests(self):
        week_end = self.week_start + timedelta(6)
        requests = pd.read_sql("""
            SELECT 
                request_id, request_date, employee_id, employee_job, employee_class, submit_date, submit_by, 
                CASE
                    WHEN DATEPART(weekday, request_date)=1 THEN 'Sun'
                    WHEN DATEPART(weekday, request_date)=2 THEN 'Mon'
                    WHEN DATEPART(weekday, request_date)=3 THEN 'Tue'
                    WHEN DATEPART(weekday, request_date)=4 THEN 'Wed'
                    WHEN DATEPART(weekday, request_date)=5 THEN 'Thu'
                    WHEN DATEPART(weekday, request_date)=6 THEN 'Fri'
                    WHEN DATEPART(weekday, request_date)=7 THEN 'Sat'
                    ELSE 'Unk'
                END As day_of_week        
            FROM app.time_off_requests
            WHERE 
                request_date BETWEEN ? and ?
                AND employee_job = 'Cashier'
            """, self.db.bind, params=(self.week_start, week_end))
        self.requests = requests
        return requests
        
    def get_cashier_summary(self):
        # Returns hours per cashier, garages worked, etc.
        ...

    def paid_hours(self, s):
        """
        Paid hours per shift (subtracts 30min break for non-SE shifts over 6.75hrs)
        """
        garage, _, _, start, end = self.shifts[s]
        duration = end - start
        #is_special_event = garage.startswith('SE')
        #if not is_special_event and duration > 6.75:
        if duration > 6.75:
            return duration - 0.5
        return duration

    def shifts_overlap(self, s1, s2):
        """
        Hard constraint: no cashier works two overlapping shifts
        Two shifts overlap if they share a day AND their time windows intersect
        """
        g1, slot1, day1, start1, end1 = self.shifts[s1]
        g2, slot2, day2, start2, end2 = self.shifts[s2]
        if day1 != day2:
            return False
        return start1 < end2 and start2 < end1

    def status_label(self):
        return {
            cp_model.OPTIMAL: 'OPTIMAL',
            cp_model.FEASIBLE: 'FEASIBLE',
            cp_model.INFEASIBLE: 'INFEASIBLE',
            cp_model.UNKNOWN: 'UNKNOWN',
            cp_model.MODEL_INVALID: 'MODEL_INVALID',
        }.get(self._solution, f'UNKNOWN({self._solution})')

    def diagnose(self):
        for s in self.shift_ids:
            garage, booth, day, start, end = self.shifts[s]
            eligible = [c for c in self.all_cashiers if day in self.availability.get(c, set(self.days))]
            if len(eligible) == 0:
                print(f"NO ELIGIBLE CASHIERS: {garage} {booth} {day} {start}-{end}")

    def print_cashier_schedule(self, cashier_id):
        cashier_schedule = defaultdict(list)
        for day, shifts in self.schedule.items():
            for garage, slot, start, end, cashier in shifts:
                cashier_schedule[cashier].append({
                    'day': day,
                    'garage': garage,
                    'slot': slot,
                    'start': start,
                    'end': end,
                    'hours': end - start
                })
        
        # Sort each cashier's shifts by day order
        day_order = {d: i for i, d in enumerate(self.days)}
        for cashier in cashier_schedule:
            cashier_schedule[cashier].sort(key=lambda x: day_order[x['day']])
    
        shifts = cashier_schedule.get(cashier_id, [])
        if not shifts:
            print(f"{cashier_id}: no shifts this week")
            return
            
        total = 0
        print(f"Schedule for {cashier_id}:")
        for s in shifts:
            print(f"  {s['day']:4s}  {s['garage']:12s} {s['slot']}  {s['start']:5.2f}-{s['end']:5.2f}  ({s['hours']:.2f} hrs)")
            total += s['hours']
        print(f"  Total: {total:.2f} hrs")
        return total

    def summary(self):
        print(f"Total shifts defined: {len(self.shifts)}")
        print(f"Total cashiers: {len(self.all_cashiers)}")
        day_counts = Counter(s[2] for s in self.shifts)
        for day in self.days:
            print(f"  {day}: {day_counts.get(day, 0)} shifts")