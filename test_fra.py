from analysis_engine import load_raw_data, load_holidays, calculate_fra_curve

df = load_raw_data()
h = load_holidays()
r = calculate_fra_curve(df, '2026-04-10', h)

print(f"Spot rates: {len(r['spot_rates'])}")
print(f"FRA rates:  {len(r['fra_rates'])}")

print("\n--- First 5 Spot Rates ---")
for s in r['spot_rates'][:5]:
    print(f"  {s['contract']:18s} rate={s['rate']:7.4f}  expiry={s['expiry']}  BD={s['bus_days']:4d}  CF={s['compound_factor']:.6f}")

print("\n--- First 5 FRA Rates ---")
for f in r['fra_rates'][:5]:
    print(f"  {f['front']:18s} -> {f['back']:18s}  D={f['period_bd']:3d}  FRA={f['fra_rate']:7.4f}%")

print("\n--- Last 3 FRA Rates ---")
for f in r['fra_rates'][-3:]:
    print(f"  {f['front']:18s} -> {f['back']:18s}  D={f['period_bd']:3d}  FRA={f['fra_rate']:7.4f}%")
