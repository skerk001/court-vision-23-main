<div align="center">

# 🏀 Courtside

### A Proprietary NBA Analytics Platform with Custom Impact Metrics

[![React](https://img.shields.io/badge/React-18.3-61dafb?style=for-the-badge&logo=react&logoColor=white)](https://reactjs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.5-3178c6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![Python](https://img.shields.io/badge/Python-3.12-ffd43b?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Flask](https://img.shields.io/badge/Flask-3.0-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-f7931e?style=for-the-badge&logo=scikit-learn&logoColor=white)](https://scikit-learn.org)

**50,000+ player-seasons · 1946–2025 · Machine Learning · Clutch Analytics**

[Live Demo](#) · [Research Paper](docs/PMI_Research_Paper.pdf) · [Architecture](#-system-architecture) · [Setup](#-quick-start)

</div>

---

## 📌 What is Courtside?

Courtside is a full-stack NBA analytics platform built around three proprietary impact metrics that evaluate every player in NBA history on a unified scale. Unlike traditional box-score aggregators, Courtside uses **era-adjusted z-score normalization**, **machine learning for historical gap-filling**, and **position-interpolated regression coefficients** to produce fair cross-era comparisons.

> *"How do you compare a 1962 Wilt Chamberlain season to a 2024 Nikola Jokić season? The same way — by measuring how far each deviated from their peers, adjusting for era inflation, and weighting by context."*

---

## ⚡ The PMI System

The **Player Metric Index (PMI)** is a composite rating that measures overall player impact on a standardized scale. It decomposes into offense and defense, supports regular season and playoffs independently, and is complemented by a clutch-specific variant.

| Metric | What It Measures | Scale |
|--------|-----------------|-------|
| **PMI** | Total player impact (OPMI + DPMI) | ~0 to +12 |
| **OPMI** | Offensive impact (scoring, efficiency, playmaking) | ~0 to +10 |
| **DPMI** | Defensive impact (stocks, rebounds, ML-imputed for pre-73) | ~0 to +5 |
| **CPMI** | Clutch Performance Metric (last 5 min, ±5 pts) | ~0 to +11 |
| **AWC** | Accumulated Win Contribution (PMI × minutes × constant) | Cumulative |

<div align="center">

![PMI Top 10](docs/assets/pmi-top10.png)

</div>

---

## 🧮 Mathematical Framework

### OPMI — Offensive Player Metric Index

For each player-season, raw box-score stats are converted to **within-season z-scores**, ensuring fair cross-era comparisons. The OPMI is a position-interpolated weighted sum:

```
OPMI_raw = Σ w(pos, stat) · clip(z_stat, -3.0, +3.0)
```

where the weight function interpolates between guard and center coefficients based on estimated position:

```
w(pos, stat) = (1 - t) · w_guard(stat) + t · w_center(stat)
    where t = (pos - 1) / 4    (PG=1, C=5)
```

**Guard Coefficients (PG/SG):**
| Component | Weight | Description |
|-----------|--------|-------------|
| `z_pts` | 1.05 | Scoring volume vs peers |
| `ts_diff` | 10.0 | True shooting % above league average |
| `z_ast` | 0.70 | Assist rate vs peers |
| `z_tov` | −0.60 | Turnover penalty |
| `z_orb` | 0.15 | Offensive rebounding |
| `z_fta` | 0.10 | Free throw drawing |
| `z_fg3m` | 0.10 | Three-point volume |

**Special Adjustments:**

```
Volume Gate:     ts_diff *= clip((z_pts + 1.0) / 2.0, 0.25, 1.0)
Center Floor:    z_pts = max(z_pts, -0.3 × max(0, (pos - 2) / 3))
Playmaker TOV:   z_tov *= (1 - min(0.30, (z_ast - 1.0) × 0.12))  if z_ast > 1.0
Era Penalty:     OPMI *= era_multiplier(season)
```

### DPMI — Defensive Player Metric Index

```
DPMI_raw = w_stl · z_stl + w_blk · z_blk + w_drb · z_drb + w_pf · z_pf
DPMI = DPMI_raw × scale × 0.72    (dampener prevents defense from dominating)
```

### Pre-1973 ML Imputation

The NBA didn't track steals or blocks before 1973. We train a **GradientBoostingRegressor** on post-1973 data to impute DPMI for historical players:

```python
Features:  trb_rate, pf_rate, team_win_pct, mpg, is_center, era
Target:    Known DPMI from post-1973 seasons
Model:     GradientBoosting(n_estimators=200, max_depth=4, lr=0.08)
```

An additional **elite historical defender boost** applies to pre-73 centers with dominant rebounding rates and exceptional team success:

```
boost = min(1.8, (trb_rate - 0.35) × 8.0 × (team_win% - 0.500) × 3.0)
```

<div align="center">

![ML DPMI Pipeline](docs/assets/ml-dpmi-pipeline.png)

</div>

### Era Stat-Inflation Penalty

Older eras had fewer teams, pace inflation, and weaker competition pools. A graduated penalty is applied to OPMI:

<div align="center">

![Era Penalty](docs/assets/era-penalty.png)

</div>

### Career Aggregation

Career PMI uses a **peak-weighted average** that reduces longevity dilution — your best seasons count more than decline years:

```
weight_i = sqrt(rank_from_best)
    where season ranked 1st gets weight √N, 2nd gets √(N-1), ..., worst gets √1

career_PMI = Σ(w_i · PMI_i) / Σ(w_i)
```

This is then **Bayesian-regressed** toward the league mean based on games played:

```
trust = GP / (GP + GP_HALF)       GP_HALF = 60 (regular) | 10 (playoffs)
final_PMI = trust × career_PMI + (1 - trust) × league_mean
```

---

## 🔥 CPMI — Clutch Performance Metric Index

A PMI-like composite built entirely from NBA clutch splits (last 5 minutes, score within ±5 points). Separate calculations for regular season and playoffs.

```
CPMI_raw = 1.50·z_ppg + 0.40·z_apg + 0.35·z_ts + 0.50·z_plusminus
         + 0.15·z_spg - 0.35·z_tovpg + volume_bonus

volume_bonus = clip((clutch_ppg - 1.5) × 0.4, 0, 1.5)
```

<div align="center">

![CPMI Comparison](docs/assets/cpmi-comparison.png)

</div>

---

## 🏆 Playoff PMI

Playoff evaluation uses a modified formula that emphasizes **scoring dominance** and reduces defensive weight — because in the playoffs, getting buckets wins series.

**Key differences from regular season:**
- `z_pts` weight: 1.05 → **1.20** (scoring matters more)
- DPMI dampener: 0.72 → **0.48** (offense-first evaluation)
- **Scoring dominance bonus**: elite z_pts (>2.5) with good efficiency gets an OPMI kicker
- GP regression half-trust: 60 → **10** games (169 playoff GP ≈ 94% trust)

<div align="center">

![Playoff PMI](docs/assets/playoff-pmi.png)

</div>

---

## 🏗 System Architecture

<div align="center">

![Architecture](docs/assets/pmi-architecture.png)

</div>

### Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | React 18 + TypeScript + Tailwind CSS | Leaderboard UI with sortable tables, heatmaps, and filters |
| **Backend** | Python 3.12 + Flask | PMI computation engine, data pipeline |
| **ML** | scikit-learn (GradientBoosting) | Pre-1973 defensive impact imputation |
| **Data** | NBA API + Basketball Reference | 34,934 player-seasons (23,991 regular + 10,943 playoff) |
| **Clutch** | NBA API Clutch Splits | 12,307 regular + 3,857 playoff clutch player-seasons |

### Data Coverage

```
📊 Historical Coverage
├── Regular Season: 1946-47 to 2024-25 (23,991 player-seasons)
├── Playoffs:       1946-47 to 2024-25 (10,943 player-seasons)
├── Clutch Regular:  1996-97 to 2024-25 (12,307 player-seasons)
└── Clutch Playoff:  1996-97 to 2024-25 (3,857 player-seasons)
```

---

## 📊 Leaderboard Features

The interactive leaderboard offers **6 tabs** across regular season and playoffs:

| Tab | Contents |
|-----|----------|
| **Per Game** | PPG, RPG, APG, SPG, BPG, TOV, FG%, TS%, rTS%, PMI, OPMI, DPMI, Peak |
| **Totals** | Counting stats, AWC, OAWC, DAWC |
| **Clutch** | CPMI with heatmaps, clutch FG%, PPG, +/−, Win% |
| **Best Season** | Peak single-season stats + PMI |
| **PMI Career** | Career PMI ratings + box score + years active |
| **PMI Season** | Best single-season PMI breakdown |

**Filters:** Season type (Regular/Playoffs) · Status (All/Active/Retired) · Minimum GP

---

## 🚀 Quick Start

### Prerequisites
- Node.js 18+
- npm or yarn

### Installation

```bash
# Clone the repository
git clone https://github.com/skerk001/court-vision-52.git courtside
cd courtside

# Install dependencies
npm install

# Start development server
npm run dev
```

The app will be available at `http://localhost:5173`.

### Build for Production

```bash
npm run build
npm run preview
```

---

## 📁 Project Structure

```
courtside/
├── src/
│   ├── components/
│   │   └── Leaderboard/
│   │       ├── DataTable.tsx       # Sortable table with heatmaps
│   │       └── FilterBar.tsx       # Season type, era, GP filters
│   ├── lib/
│   │   ├── constants.ts            # Column definitions, tab configs
│   │   ├── formatters.ts           # Number formatting, heat coloring
│   │   └── mockData.ts             # 100 players × 2 season types
│   ├── pages/
│   │   └── Leaderboard.tsx         # Main leaderboard page
│   └── App.tsx
├── docs/
│   ├── assets/                     # Architecture diagrams, charts
│   └── PMI_Research_Paper.pdf      # Full methodology paper
└── README.md
```

---

## 📝 Version History

| Version | Changes |
|---------|---------|
| **v41e** | Separate playoff CPMI from NBA API playoff clutch splits |
| **v41d** | Playoff PMI scale boost (15.5x), GP regression fix (half=10) |
| **v41c** | Scoring dominance bonus, playoff DPMI dampener (0.48) |
| **v41b** | Era penalty applied to playoff OPMI, Kareem fix |
| **v41** | Volume gate for ts_diff, playmaking rebalance, ML defender boost |
| **v40** | Peak-weighted career avg, playmaker TOV discount |
| **v39b** | Center scoring floor, team offensive context for Bill Russell |

---

## 🔬 Research

For a detailed methodology paper covering the statistical framework, ML pipeline, validation, and cross-era comparison analysis, see the [PMI Research Paper](docs/PMI_Research_Paper.pdf).

---

## 📜 License

This project is proprietary. All rights reserved. The PMI methodology, source code, and analytics framework are the intellectual property of Samir Kerkar. Viewing is permitted for portfolio evaluation purposes only.
---

<div align="center">

**Built by [Samir Kerkar](https://github.com/skerk001)**

*Courtside — where data meets the game.*

</div>
