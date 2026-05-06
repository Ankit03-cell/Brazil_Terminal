from analysis_engine import load_holidays
from datetime import date, timedelta
hol = load_holidays()
c = 0
hols_skipped = []
d = date(2026, 4, 11)
end = date(2028, 1, 3)

while d <= end:
    if d.weekday() < 5:
        if d in hol:
            hols_skipped.append(d)
        else:
            c += 1
    d += timedelta(days=1)
    
print('Calculated bd:', c)
print('Holidays skipped:', len(hols_skipped))
for h in hols_skipped:
    print("  - ", h)
