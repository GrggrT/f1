from __future__ import annotations

import json
import random

from data.database import Database
from services.budget import get_driver_name


ROASTS = [
    "Может стоило просто выбрать рандомно? \U0001f3b2",
    "Спонсирует DNF-фонд \U0001f480",
    "Fantasy is not your thing \U0001f62c",
    "Антистратег сезона \U0001f9e0\u274c",
    "Даже бот справился бы лучше \U0001f916",
    "Главный донор лиги \U0001f4b8",
    "Picks настолько плохие, что даже пит-стоп Williams быстрее \U0001f422",
]


class AwardsEngine:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def calculate_round_awards(self, race_round: int) -> list[dict]:
        """Calculate all awards for a race round."""
        scores = await self.db.get_race_scores(race_round)
        if not scores:
            return []

        awards = []

        # Enrich with usernames
        score_data = []
        for s in scores:
            user = await self.db.get_user(s.user_id)
            username = user["username"] if user and user.get("username") else str(s.user_id)
            score_data.append({
                "user_id": s.user_id,
                "username": username,
                "points": s.fantasy_points,
                "breakdown": s.breakdown,
            })

        score_data.sort(key=lambda x: x["points"], reverse=True)

        # Best Manager
        best = score_data[0]
        awards.append({
            "emoji": "\U0001f3c6",
            "title": "Manager of the Round",
            "user": best["username"],
            "description": f"{best['points']:.0f} pts",
        })

        # Worst Manager
        if len(score_data) > 1:
            worst = score_data[-1]
            roast = random.choice(ROASTS)
            awards.append({
                "emoji": "\U0001f4a9",
                "title": "Antimanager",
                "user": worst["username"],
                "description": f"{worst['points']:.0f} pts \u2014 {roast}",
            })

        # DRS Master — best turbo pick
        best_turbo = None
        best_turbo_bonus = 0
        for s in score_data:
            bd = s["breakdown"]
            if isinstance(bd, str):
                bd = json.loads(bd)
            turbo_driver = bd.get("turbo_driver", "")
            drivers_bd = bd.get("drivers", {})
            turbo_bd = drivers_bd.get(turbo_driver, {})
            turbo_bonus = turbo_bd.get("turbo_bonus", 0)
            if turbo_bonus > best_turbo_bonus:
                best_turbo_bonus = turbo_bonus
                best_turbo = s

        if best_turbo and best_turbo_bonus > 0:
            bd = best_turbo["breakdown"]
            if isinstance(bd, str):
                bd = json.loads(bd)
            turbo_name = get_driver_name(bd.get("turbo_driver", ""))
            awards.append({
                "emoji": "\u26a1",
                "title": "DRS Master",
                "user": best_turbo["username"],
                "description": f"+{best_turbo_bonus:.0f} pts from {turbo_name}",
            })

        # Galaxy Brain — most unique pick that scored well
        # Find driver picked by fewest users who scored the most
        teams = await self.db.get_all_teams_for_round(race_round)
        if teams:
            driver_counts: dict[str, int] = {}
            for t in teams:
                for d in t.drivers:
                    driver_counts[d] = driver_counts.get(d, 0) + 1

            # Find user with best-scoring contrarian pick
            best_contrarian = None
            best_contrarian_score = 0
            for s in score_data:
                bd = s["breakdown"]
                if isinstance(bd, str):
                    bd = json.loads(bd)
                for driver_id, driver_score in bd.get("drivers", {}).items():
                    if not isinstance(driver_score, dict):
                        continue
                    count = driver_counts.get(driver_id, 0)
                    pts = driver_score.get("total", 0)
                    # Contrarian = picked by only 1 user and scored well
                    if count == 1 and pts > best_contrarian_score:
                        best_contrarian_score = pts
                        best_contrarian = {
                            "username": s["username"],
                            "driver": driver_id,
                            "pts": pts,
                        }

            if best_contrarian and best_contrarian_score > 10:
                awards.append({
                    "emoji": "\U0001f9e0",
                    "title": "Galaxy Brain",
                    "user": best_contrarian["username"],
                    "description": (
                        f"{get_driver_name(best_contrarian['driver'])} "
                        f"({best_contrarian['pts']:.0f} pts, unique pick)"
                    ),
                })

        # On a streak — consecutive top-2 finishes
        for s in score_data[:2]:
            streak = await self._get_streak(s["user_id"], race_round)
            if streak >= 3:
                awards.append({
                    "emoji": "\U0001f525",
                    "title": "On Fire",
                    "user": s["username"],
                    "description": f"{streak} rounds in a row in top-2!",
                })

        return awards

    async def _get_streak(self, user_id: int, current_round: int) -> int:
        """Count consecutive rounds where user finished in top 2."""
        streak = 0
        for r in range(current_round, 0, -1):
            scores = await self.db.get_race_scores(r)
            if not scores:
                break
            scores_sorted = sorted(scores, key=lambda x: x.fantasy_points, reverse=True)
            top2_ids = [s.user_id for s in scores_sorted[:2]]
            if user_id in top2_ids:
                streak += 1
            else:
                break
        return streak

    async def generate_h2h_update(self) -> str:
        """Generate head-to-head records between all user pairs."""
        users = await self.db.get_all_users()
        if len(users) < 2:
            return ""

        # Get all rounds with scores
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT race_round FROM scores ORDER BY race_round"
            )
        rounds = [row["race_round"] for row in rows]
        if not rounds:
            return ""

        # Calculate H2H for each round
        h2h: dict[tuple, list[int, int]] = {}
        for r in rounds:
            scores = await self.db.get_race_scores(r)
            score_map = {s.user_id: s.fantasy_points for s in scores}
            user_ids = list(score_map.keys())
            for i in range(len(user_ids)):
                for j in range(i + 1, len(user_ids)):
                    pair = (user_ids[i], user_ids[j])
                    if pair not in h2h:
                        h2h[pair] = [0, 0]
                    if score_map[user_ids[i]] > score_map[user_ids[j]]:
                        h2h[pair][0] += 1
                    elif score_map[user_ids[j]] > score_map[user_ids[i]]:
                        h2h[pair][1] += 1

        if not h2h:
            return ""

        # Find interesting matchups (close or notable)
        user_map = {u["telegram_id"]: u.get("username", str(u["telegram_id"])) for u in users}
        lines = ["\U0001f91c *Head-to-Head*\n"]

        for (u1, u2), (w1, w2) in sorted(h2h.items(), key=lambda x: min(x[1]), reverse=True):
            name1 = user_map.get(u1, str(u1))
            name2 = user_map.get(u2, str(u2))
            diff = abs(w1 - w2)
            hot = "\U0001f525 " if diff <= 1 and (w1 + w2) >= 3 else ""
            lines.append(f"{hot}@{name1} vs @{name2}: {w1}-{w2}")

        return "\n".join(lines)

    async def generate_rival_h2h(self, race_round: int) -> str:
        """Generate H2H update specifically for registered rivals."""
        async with self.db.pool.acquire() as conn:
            rival_pairs = await conn.fetch(
                "SELECT DISTINCT user_id, rival_id FROM h2h_rivals"
            )
        if not rival_pairs:
            return ""

        lines = ["\U0001f91c *Rival H2H Update*\n"]
        for pair in rival_pairs:
            user_id, rival_id = pair[0], pair[1]
            record = await self.db.get_h2h_record(user_id, rival_id)
            if not record["rounds"]:
                continue
            # Find this round's result
            round_result = next((r for r in record["rounds"] if r["race_round"] == race_round), None)
            if not round_result:
                continue

            user = await self.db.get_user(user_id)
            rival = await self.db.get_user(rival_id)
            u_name = f"@{user['username']}" if user and user.get("username") else str(user_id)
            r_name = f"@{rival['username']}" if rival and rival.get("username") else str(rival_id)

            if round_result["user_pts"] > round_result["rival_pts"]:
                winner = u_name
            elif round_result["rival_pts"] > round_result["user_pts"]:
                winner = r_name
            else:
                winner = "Ничья!"

            lines.append(
                f"{u_name} vs {r_name}: "
                f"{round_result['user_pts']:.0f}-{round_result['rival_pts']:.0f} "
                f"(Сезон: {record['wins']}-{record['losses']}-{record['draws']}) "
                f"\u2192 {winner}"
            )

        return "\n".join(lines) if len(lines) > 1 else ""

    def generate_roast(self, username: str, score: float) -> str:
        roast = random.choice(ROASTS)
        return f"@{username} ({score:.0f} pts) \u2014 {roast}"
