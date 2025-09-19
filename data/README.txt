GlobalStay – 5 Tables Lean Dataset (CSV) with embedded anomalies
Generated: 2025-08-26

Tables:
1) hotels.csv (n=8) – master hotel (1 invalid country code 'XX')
2) rooms.csv (n=201) – rooms per hotel (contains 1 duplicated row)
3) customers.csv (n=3001) – customer master (blank emails at regular intervals, 1 duplicate row)
4) bookings.csv (n=6000) – reservations with anomalies:
   - negative totals (few rows)
   - swapped dates (check-in > check-out)
   - currency 'XXX' invalid (few rows)
   - implicit overlaps (overbooking) on same room and overlapping dates
5) payments.csv (n=4595) – transactions with anomalies:
   - amount > booking total (few rows)
   - invalid currency 'ZZZ' (few rows)
   - orphan booking_id (e.g. B999999)

Suggested tasks:
- Bronze: ingestion 1:1
- Silver: cleaning (fix/swaps, null policy, FK checks, dedup)
- Gold: KPIs: daily revenue, cancellation rate, collection rate, overbooking alerts, customer value

