# Headline insights

Computed from the 29 Oct 2026 snapshot. Times are UTC. Australian Eastern Daylight Time is UTC+11; Adelaide is UTC+10:30.

## The Gippsland rate is unusual. The national rate is not.

In a box around Leongatha (38.70–38.15°S, 145.55–146.30°E), Geoscience Australia records **14 earthquakes of magnitude 2 or greater** between 26 Sep 2026 and the newest event on 28 Oct (32 days), plus one smaller event (15 in total). Equal-length windows stepped through 2006–Sep 2026 have a **median of 0**, a **95th percentile of 2**, and a **99th percentile of 8**. The current window is ranked **unusual**.

The same box over the last 30 days is also unusual: 14 events against a 99th percentile of about 6.5. The last 7 days has 2 events, which meets the 99th percentile of a zero-heavy weekly distribution. That weekly flag is real but thin. The 30-day and 32-day comparisons are the ones to use.

Victoria as a whole, last 30 days, magnitude ≥ 2: **16 events** against a median of 1 and a 99th percentile of 12 (**unusual**). Since 26 Sep the state polygon contains **19 events of any positive magnitude**. The ABC described the 28 Oct article's M3.5 as the 18th Victorian quake since 26 Sep. The catalogue is one event higher, which is the sort of difference a magnitude cutoff or a revised solution produces.

Australia-wide, magnitude ≥ 2, the last 7 days (10 events, median 11) and last 30 days (71 events, median 49, 95th percentile 101) are **typical**. The swarm is a local rate change, not a national one.

## What the sequence actually contains

| UTC time | Local (AEDT, UTC+11) | Magnitude | Place |
| --- | --- | --- | --- |
| 2026-10-02 16:01 | 3 Oct 03:01 | 4.9 MLa05 | 8 km NW of Leongatha |
| 2026-10-27 12:52 | 27 Oct 23:52 | 3.5 MLa075 | 8 km NW of Leongatha |
| 2026-10-28 08:14 | 28 Oct 19:14 | 2.4 MLa075 | 12 km NW of Leongatha |

GA's preferred magnitude for the 3 Oct event is **MLa 4.9**. The ABC reported it as about M4.8. Those are different magnitude types, not a contradiction to paper over. The M3.5 origin is still 27 Oct in Melbourne local time; the ABC story is dated 28 Oct, which matches an overnight event filed the next morning.

Depths in the box for this window are shallow where GA estimated them: about 2.5 to 10 km. Several are exactly 10 km, a common default, so the depth comparison is weaker than the rate comparison.

ST-DBSCAN (25 km, 7 days, at least 4 neighbours) puts the early-October events in **one cluster**. The 27–28 Oct events sit just outside that 7-day neighbourhood and are labelled noise by the algorithm. They still count in the rate window. This is a clustering rule, not a claim that the later events are unrelated.

## Gutenberg-Richter

For ML-family magnitudes in the current window, maximum curvature plus 0.2 gives **Mc = 2.5**. The Aki / Utsu estimate is **b ≈ 0.51 on 7 events**. That sample is below the 50-event line used here for a stable estimate. The b-value is reported and then set aside. The count comparison is the result that can carry weight.

## Hotspots are a 72-hour picture, not a season

The public DEA WFS layer `hotspots_three_days` returned **157,822 detections** from 26 Oct 10:36 UTC to 29 Oct 10:18 UTC. Himawari repeats every 10 minutes, so this is detections, not fires. In the newest 6 hours of that feed the dashboard counts both raw detections and distinct H3 resolution-6 cells.

By state, after trimming DEA's abbreviations: Queensland 41,624, Northern Territory 27,536, Western Australia 21,587, New South Wales 1,581, South Australia 460, Victoria 458, Tasmania 214, ACT 4. **64,358** have no state (ocean, or outside the simplified polygons).

**1,831** detections are within 25 km of a Natural Earth place with `pop_max` ≥ 10,000. **2,338** are within 20 km of a medium or large airport. **121** are within 25 km of Perth Airport (YPPH). That is consistent with thermal detections in the Perth Hills while the Kalamunda fire was being reported. A hotspot is not a confirmed fire, and this dashboard is not a warning.

There is no key-free multi-month hotspot archive on the public WFS, so a seasonal trend beyond these 72 hours is not computed.

## How fast the feeds publish

| Feed | What the lag measures | n | Median | 95th percentile |
| --- | --- | --- | --- | --- |
| Geoscience Australia | creation time minus origin, last 30 days, lags under 7 days | 55 | 1.1 min | 15.6 min |
| USGS | last update minus origin (an upper bound, not the first alert) | 8 | 35 min | ~18 h |
| DEA Hotspots | processing time minus observation, lags under 24 h | 139,259 | 36 min | 59 min |

At ingest (29 Oct 10:36 UTC) the newest hotspot was **18 minutes** old. The newest GA event was about **11 hours** old, which is a quiet stretch of the catalogue, not a stuck download. Historical GA rows often carry a database load time years after the origin; those lags are excluded from the SLA table.
