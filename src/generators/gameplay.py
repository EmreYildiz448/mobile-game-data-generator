import random
import math
import itertools
from datetime import datetime, timedelta, date
from collections import defaultdict, deque

from src import catalogs as C
from src.settings import runtime as R
from src.event_handler import EventHandler
from src.analytics import AnalyticsFramework
from src.generators.chest_handler import ChestHandler
from src.generators.errors import ErrorGenerator
from src.generators.ad_events import AdEventGenerator
from src.generators.business import BusinessEventGenerator
from src.generators.ig_purchases import InGamePurchaseGenerator

class EventGenerator:
    def __init__(self, account_map_data=None, 
                 level_data=None, item_data=None, 
                 shop_offers=None, ad_campaigns=None, 
                 chest_handler=None, error_data=None, 
                 error_map=None, worker_id=None):
        if account_map_data is None:
            raise ValueError("account_map_data must be provided.")
        if ad_campaigns is None:
            raise ValueError("ad_campaigns must be provided.")
        if chest_handler is None:
            raise ValueError("chest_handler must be provided.")

        self.worker_id = worker_id

        self.ab_assignment_device_counter = defaultdict(lambda: {'total': 0, 'test': defaultdict(int)})
        self.processed_ab_accounts = set()
        self.reverted_ab_accounts = set()
        self.event_handler = EventHandler(C.event_master_dict)  # Initialize EventHandler
        self.account_map_data = {account['account_id']: account for account in account_map_data}
        level_data_src = level_data if level_data is not None else C.level_data
        self.item_data = item_data if item_data is not None else C.item_data
        self.level_data = {k: v for k, v in level_data_src.items() if not k.startswith("TUTORIAL")}
        self.tutorial_levels = {k: v for k, v in level_data_src.items() if     k.startswith("TUTORIAL")}
        self.shop_offers = shop_offers
        self.ad_campaigns = ad_campaigns
        self.error_data = error_data
        self.error_map = error_map
        self.item_success_contributions = C.item_success_contributions
        self.analytics = AnalyticsFramework(max_days=R.ANALYTICS_MAX_DATERANGE)  # Initialize the AnalyticsFramework
        self.error_generator = ErrorGenerator(self.error_data, self.error_map, self.event_handler)
        self.chest_handler = ChestHandler(
            self.item_data, self.event_handler, self.error_generator)
        self.ad_event_generator = AdEventGenerator(
            self.ad_campaigns, self.event_handler, self.error_generator)
        self.business_event_generator = BusinessEventGenerator(
            self.shop_offers, self.chest_handler, self.event_handler, self.error_generator, self.analytics)
        self.in_game_purchase_generator = InGamePurchaseGenerator(
            self.shop_offers, self.item_data, self.chest_handler, self.event_handler, self.error_generator, self.analytics)
        self.events = deque()
        self.account_states = {}
        self.equipped_items = {}  # Store equipped items for accounts
        self.final_account_states = []
        self._ensure_level_diffmult()

        # Per-account gear cache: parsed items, per-item bonus, best loadout, and a dirty flag
        self._gear_cache = defaultdict(lambda: {
            "parsed": {},   # item_id -> (rarity, base)    (str, str)
            "bonus":  {},   # item_id -> float             (precomputed item contribution)
            "best":   {},   # slot -> item_id              (weapon/armor/held_item/skin/hero)
            "sum_bonus": 0.0,
            "dirty":  True,
        })

    @staticmethod
    def _parse_item_id(item_id: str):
        # e.g. "e_hi_amulet" -> rarity="e", cat="hi", base="amulet"
        parts = str(item_id).split("_", 2)
        if len(parts) < 3:
            return "c", "unknown", "unknown"
        rarity, cat, base = parts
        # inventory-slot keys used in account_state["equipment"]
        inv_slot = {"wp": "weapons", "hi": "held_items", "arm": "armor", "s": "skins"}.get(cat, "unknown")
        return rarity, inv_slot, base

    def _item_bonus(self, rarity: str, inv_slot: str, base: str) -> float:
        """
        Map inventory slot -> (contrib slot, prefix), then multiply base * rarity.
        """
        isc = self.item_success_contributions  # alias
        rarity_mult = (isc.get("rarity") or {}).get(rarity, 1.0)

        # inventory slot -> (contribution slot in catalogs, item-id prefix)
        slot_map = {
            "weapons":    ("weapons",    "wp"),
            "held_items": ("held_items", "hi"),
            "armor":      ("armors",     "arm"),   # <-- catalogs uses "armors" (plural)
        }
        contrib_slot, prefix = slot_map.get(inv_slot, ("", ""))

        base_key = f"{prefix}_{base}"  # e.g. "hi_amulet", "arm_rune", "wp_staff"
        base_w = ((isc.get("equipment") or {}).get(contrib_slot) or {}).get(base_key, 0.0)

        return base_w * rarity_mult

    def _mark_gear_dirty(self, account_id: int):
        self._gear_cache[account_id]["dirty"] = True

    def _ensure_best_gear_cached(self, account_id: int, account_state: dict):
        cache = self._gear_cache[account_id]
        if not cache["dirty"]:
            return cache

        parsed = cache["parsed"]
        bonus  = cache["bonus"]
        best   = {}
        total  = 0.0

        equipment = account_state.get("equipment", {})

        # rarity weights once
        rarity_w = (self.item_success_contributions.get("rarity") or {})

        # Only these slots are “canonical” equipment
        for slot_key in ("weapons", "armor", "held_items"):
            owned = equipment.get(slot_key, {})

            # choose by RARITY ONLY
            best_item, best_rarity_score = None, float("-inf")

            for item_id, count in owned.items():
                if count <= 0:
                    continue

                # parse once
                if item_id not in parsed:
                    parsed[item_id] = self._parse_item_id(item_id)  # (rarity, inv_slot, base)
                rarity, inv_slot_from_id, base = parsed[item_id]
                if inv_slot_from_id != slot_key:
                    continue

                rarity_score = rarity_w.get(rarity, 0.0)  # <— selection criterion

                if rarity_score > best_rarity_score:
                    best_rarity_score = rarity_score
                    best_item = item_id

                # precompute full contribution (rarity × base) for success later
                if item_id not in bonus:
                    bonus[item_id] = self._item_bonus(rarity, slot_key, base)

            if best_item is not None:
                best[slot_key] = best_item
                total += bonus.get(best_item, 0.0)  # success still uses full contrib

        cache["best"] = best
        cache["sum_bonus"] = total
        cache["dirty"] = False
        return cache

    def _precompute_level_diffmult(self) -> list[float]:
        """
        Precompute difficulty multipliers for levels 1..R.LATEGAME_MAX_LEVEL
        using the same exact formula/thresholds as calculate_success_chance().
        Index i corresponds to level i (0 unused).
        """
        max_level = R.LATEGAME_MAX_LEVEL
        arr = [0.0] * (max_level + 1)
        min_value = R.DIFFMULT_MIN_VALUE
        max_value = R.DIFFMULT_MAX_VALUE
        exp = R.DIFFMULT_GROWTH_EXPONENT

        for lvl in range(1, max_level + 1):
            mult = min_value + (max_value - min_value) * ((lvl - 1) / (max_level - 1)) ** exp
            # Apply the same flat mid/late modifiers
            if R.MIDGAME_MIN_LEVEL <= lvl <= R.MIDGAME_MAX_LEVEL:
                mult *= R.MIDGAME_BASE_DIFFMULT
            elif R.LATEGAME_MIN_LEVEL <= lvl <= max_level:
                mult *= R.LATEGAME_BASE_DIFFMULT
            arr[lvl] = mult
        return arr

    def _ensure_level_diffmult(self):
        if not hasattr(self, "_level_diffmult"):
            self._level_diffmult = self._precompute_level_diffmult()

    def initialize_account_state(self, account_id):
        if account_id not in self.account_states:
            self.account_states[account_id] = {
                "account_id": account_id,
                "account_archetype": None,
                "total_gold": 0,
                "total_diamond": 0,
                "last_completed_level": 0,
                "last_session_date": None,
                "last_retention_day": None,
                "success_streak": 0,
                "failure_streak": 0,
                "recent_engagement_event": None,
                "equipment": {
                    "heroes": {},
                    "skins": {},
                    "weapons": {},
                    "held_items": {},
                    "armor": {}
                },
                "chests": {},
                "equipped_items": {
                    "equipped_hero": None,
                    "equipped_skin": None,
                    "equipped_weapon": None,
                    "equipped_held_item": None,
                    "equipped_armor": None
                },
                "total_wins": 0,
                "total_losses": 0,
                "total_money_spent": 0,
                "session_initialized": False,
                "full_churn": None,
                "churn_count": 0,
                "churned": "False",
                "churn_log": [],
                "is_subscribed": None,
                "subscription_date": None,
                "sessions_today": 0
            }
        self.account_states[account_id]["total_gold"] = max(0, self.account_states[account_id]["total_gold"])
        self.account_states[account_id]["total_diamond"] = max(0, self.account_states[account_id]["total_diamond"])

    def determine_equipment(self, account_id):
        """
        Equip best cached gear for this account. Recomputes only if inventory changed.
        """
        account_state = self.account_states[account_id]

        # If any producer (chests/purchases/combine) marked the inventory dirty, reflect that
        if account_state.get("__gear_dirty__", False):
            self._mark_gear_dirty(account_id)
            account_state["__gear_dirty__"] = False

        cache = self._ensure_best_gear_cached(account_id, account_state)

        # Map cached best items into your equipped_items structure
        equipped_items = account_state["equipped_items"]
        equipped_items["equipped_weapon"] = cache["best"].get("weapons")
        equipped_items["equipped_held_item"] = cache["best"].get("held_items")
        equipped_items["equipped_armor"] = cache["best"].get("armor")
    
        # Determine equipped hero and skin
        owned_heroes = account_state["equipment"].get("heroes", {})
        owned_skins = account_state["equipment"].get("skins", {})
    
        # Prioritize heroes that have skins
        hero_skin_count = {
            hero: len([skin for skin in owned_skins if skin in self.item_data["skins"].get(hero, [])])
            for hero in owned_heroes
        }
        equipped_hero = max(hero_skin_count, key=lambda h: hero_skin_count[h], default=None)
    
        equipped_items["equipped_hero"] = equipped_hero
    
        # Equip a skin if the hero is equipped and skins are available
        if equipped_hero:
            available_skins = [
                skin for skin in owned_skins if skin in self.item_data["skins"].get(equipped_hero, [])
            ]
            equipped_skin = random.choice(available_skins) if available_skins else None
    
        equipped_items["equipped_skin"] = equipped_skin
    
    def calculate_success_chance(self, account_id, level_cfg, current_level):
        """
        Calculate success chance based on player skill, difficulty, and equipped items.
        """
        account_data_ext = self.account_map_data[account_id]
        player_skill_interval = account_data_ext.get("player_skill", (1.0, 1.0))
        player_skill = random.uniform(*player_skill_interval)

        # O(1) difficulty multiplier lookup instead of recomputing each time
        self._ensure_level_diffmult()
        difficulty_multiplier = self._level_diffmult[current_level]

        # same as before: level difficulty * level_cfg['difficulty']
        difficulty = level_cfg.get("difficulty", 1.0) * difficulty_multiplier
        
        # Base success chance
        success_chance = player_skill / difficulty

        # Get equipped items' contributions
        # Use cached equipment contribution
        account_state = self.account_states[account_id]

        # Pick up any pending inventory changes and refresh cache if needed
        if account_state.get("__gear_dirty__", False):
            self._mark_gear_dirty(account_id)
            account_state["__gear_dirty__"] = False
        cache = self._ensure_best_gear_cached(account_id, account_state)

        equip_bonus = cache["sum_bonus"]  # fast path: sum of per-slot best bonuses (weapon/armor/held_item)
        success_chance += equip_bonus

        overpowered = self.item_success_contributions["overpowered"]
        underpowered = self.item_success_contributions["underpowered"]

        for category in ["equipped_weapon", "equipped_held_item", "equipped_armor", "equipped_skin"]:
            item_id = account_state["equipped_items"].get(category)
            if not item_id:
                continue

            # Skins are special; keep your existing skin bonus logic if needed elsewhere.
            if item_id.startswith("s_"):
                # no rarity/base parsing for skins
                pass
            else:
                # Get rarity/base from cache (parse once)
                parsed = self._gear_cache[account_id]["parsed"]
                if item_id not in parsed:
                    parsed[item_id] = self._parse_item_id(item_id)
                rarity, inv_slot, base = parsed[item_id]
                prefix = {"weapons": "wp", "held_items": "hi", "armor": "arm"}.get(inv_slot, "")
                base_id = f"{prefix}_{base}"

                if base_id == overpowered["item_id"]:
                    success_chance *= R.OP_ITEM_SUCCESS_FACTOR
                if base_id == overpowered["rarity_combination"]["item_id"] and rarity == overpowered["rarity_combination"]["rarity"]:
                    success_chance *= R.OP_COMBO_SUCCESS_FACTOR

                if base_id == underpowered["item_id"]:
                    success_chance *= R.UP_ITEM_SUCCESS_FACTOR
                if base_id == underpowered["rarity_combination"]["item_id"] and rarity == underpowered["rarity_combination"]["rarity"]:
                    success_chance *= R.UP_COMBO_SUCCESS_FACTOR

        return round(min(success_chance, 1.0), 3)

    def calculate_shop_activity_probability(self, account_id):
        """
        Calculate dynamic shop activity probability for a specific account.
    
        Separate gold and diamond effects to simulate diamond-saving behavior.
    
        Returns:
            float: The calculated shop activity probability.
        """
        account_state = self.account_states.get(account_id, {})
        account_data_ext = self.account_map_data[account_id]
    
        # Base probability from archetype
        base_probability = account_data_ext.get("shop_activity_probability", 0.1)
    
        # Gold-based adjustment
        total_gold = account_state.get("total_gold", 0)
        gold_upper_limit = account_data_ext.get("gold_upper_limit", 1000)  # Archetype-specific
        gold_factor = min(total_gold / gold_upper_limit, 1.0)
    
        # Diamond-based adjustment
        total_diamonds = account_state.get("total_diamond", 0)
        diamond_threshold = account_data_ext.get("diamond_upper_limit", 500)
        diamond_factor = 0 if total_diamonds < diamond_threshold else min((total_diamonds - diamond_threshold) / 1000, 0.3)
    
        # Level failures-based adjustment
        level_failures = account_state.get("level_failures", 0)
        failure_factor = min(level_failures * 0.05, 0.3)  # Max +30% for failures
    
        # Missing equipment adjustment
        missing_equipment_categories = sum(
            1 for category in ["weapons", "held_items", "armor"]
            if not account_state["equipment"].get(category)
        )
        equipment_factor = missing_equipment_categories * 0.1  # +10% per missing category
    
        # Total probability
        shop_activity_probability = base_probability + gold_factor + diamond_factor + failure_factor + equipment_factor
        return min(shop_activity_probability, 1.0)

    def calculate_retention_probability(self, account_id, current_date):
        account_state = self.account_states.get(account_id, {})
        account_data_ext = self.account_map_data[account_id]
        last_session_date = account_state["last_session_date"]
        base_retention = account_data_ext.get("retention_probability", R.DEFAULT_RETENTION_BASE)
        days_since_last_active = (current_date - last_session_date).days if last_session_date else 0
    
        # Progress Influence
        levels_completed = account_state.get("last_completed_level", 0)
        progress_bonus = min(R.PROGRESS_BONUS_PER_LEVEL * levels_completed, R.PROGRESS_BONUS_CAP)
    
        # Investment Modifier
        total_spent = account_state.get("total_money_spent", 0)
        total_gold = account_state.get("total_gold", 0)
        total_diamonds = account_state.get("total_diamond", 0)
    
        rarity_weights = C.rarity_retention_weights
        inventory_value = sum(
            rarity_weights.get(item.split("_")[0], 1.0) * count
            for category in account_state["equipment"].values()
            for item, count in category.items()
        )
    
        currency_factor = min((total_gold + total_diamonds) / R.CURRENCY_DIVISOR, R.CURRENCY_FACTOR_CAP)
        inventory_factor = min(inventory_value / R.INVENTORY_DIVISOR , R.INVENTORY_FACTOR_CAP)
        investment_bonus = min((total_spent / R.SPEND_DIVISOR ) + inventory_factor + currency_factor, R.INVESTMENT_BONUS_CAP)
    
        # Win/Loss Influence
        total_wins = account_state.get("total_wins", 0)
        total_losses = account_state.get("total_losses", 0)
        win_loss_ratio = total_wins / max(total_losses, 1)
        win_bonus = max(-0.2, min(0.15, 0.1 * (win_loss_ratio - 1)))
    
        # Decay Factor
        decay_constant = account_data_ext.get("decay_constant", R.DEFAULT_DECAY_CONSTANT)
        decay_penalty = base_retention * (1 - math.exp(-decay_constant * days_since_last_active))
    
        final_retention = base_retention + progress_bonus + investment_bonus + win_bonus - decay_penalty * R.DECAY_MULTIPLIER
        return max(0.0, min(final_retention, 1.0))

    def calculate_full_churn(self, account_state, rp_check, retention_probability):
        churn_count = account_state["churn_count"]
        churn_reduction = (rp_check - retention_probability) * (churn_count ** R.CHURN_EXPONENT ) * R.CHURN_REDUCTION_MULT if churn_count != 0 else (rp_check - retention_probability)
        
        original_full_churn = account_state["churn_log"][0]
        
        rarity_weights = C.rarity_retention_weights
        inventory_size = sum(
            rarity_weights.get(item.split("_")[0], 1.0) * count
            for category in account_state["equipment"].values()
            for item, count in category.items()
        )
    
        buffer = (
            R.BUF_SUCCESS_STREAK_STEP * min(account_state.get("success_streak", 0), R.BUF_SUCCESS_STREAK_CAP) +
            R.BUF_LEVEL_STEP * min(account_state.get("last_completed_level", 0), R.BUF_LEVEL_CAP) +
            R.BUF_SPENT_STEP * min(account_state.get("total_money_spent", 0), R.BUF_SPENT_CAP) +
            R.BUF_INVENTORY_STEP * min(inventory_size, R.BUF_INVENTORY_CAP)
        )
    
        penalty = R.PENALTY_FAIL_STREAK_STEP * min(account_state.get("failure_streak", 0), R.PENALTY_FAIL_STREAK_CAP)
        adjusted_churn_reduction = min(max(churn_reduction - buffer + penalty, 0), original_full_churn * R.MAX_CHURN_REDUCTION_FRAC)
    
        account_state["full_churn"] -= adjusted_churn_reduction
        account_state["full_churn"] = max(account_state["full_churn"], 0)

        final_churn_value = account_state["full_churn"]
        account_state["churn_log"].append(final_churn_value)
#        print(f"Initial churn reduction: {churn_reduction}")
#        print(f"Total buffer: {buffer}")
#        print(f"Penalty: {penalty}")
#        print(f"Adjusted churn reduction: {adjusted_churn_reduction}")
#        print(f"Final full_churn value: {final_churn_value}")
    
    def calculate_session_termination_probability(self, account_id, session_duration, success_streak, failure_streak, current_timestamp, recent_engagement_event):
        """
        Calculate the dynamic session termination probability for a player.
        
        Args:
            account_id (str): The ID of the account.
            session_duration (float): Duration of the current session in minutes.
            success_streak (int): Number of consecutive successful levels.
            failure_streak (int): Number of consecutive failed levels.
            current_timestamp (datetime): The current timestamp during the session.
            recent_engagement_event (bool | None): 
                - True: Completed reward ad (slight decrease in termination chance).
                - False: Skipped regular ad (slight increase in termination chance).
                - None: No impact.
        
        Returns:
            float: The calculated session termination probability.
        """
        account_data_ext = self.account_map_data[account_id]
        base_termination_probability = account_data_ext.get("session_termination_probability", R.DEFAULT_TERMINATION_BASE)
    
        # Fatigue factor: Increases with session duration
        fatigue_factor = min(R.FATIGUE_SLOPE_PER_MIN * session_duration, R.FATIGUE_CAP)  # +1% per minute, cap at 20%
    
        # Progression factor: Increases with consecutive failures, decreases with successes
        progression_factor = min(failure_streak * R.FAIL_STREAK_STEP, R.FAIL_STREAK_CAP) - min(success_streak * R.SUCCESS_STREAK_STEP, R.SUCCESS_STREAK_CAP)
    
        # Schedule factor: Based on time of day (e.g., higher during interruptions like meal times)
        hour = current_timestamp.hour
        schedule_factor = R.SCHEDULE_SINE_AMPLITUDE * math.sin(2 * math.pi * (hour / 24))  # Sinusoidal variation over 24 hours
    
        # Engagement factor: Adjust based on recent ad engagement
        if recent_engagement_event is True:
            engagement_factor = R.ENGAGEMENT_COMPLETED_DELTA  # Decrease by 5% for completed reward ads
        elif recent_engagement_event is False:
            engagement_factor = R.ENGAGEMENT_SKIPPED_DELTA  # Increase by 5% for skipped regular ads
        else:
            engagement_factor = 0.0  # No impact
    
        # Final probability calculation
        session_termination_probability = (
            base_termination_probability +
            fatigue_factor +
            progression_factor +
            schedule_factor +
            engagement_factor
        )
        return round(min(max(session_termination_probability, 0.0), 1.0), 2)  # Clamp between 0 and 1

    def calculate_return_date(self, account_id, last_session_end):
        """
        Determines the next session's start time based on retention probability and engagement factors.
        
        Args:
            account_id (str): The unique identifier for the player.
            last_session_end (datetime): The timestamp when the last session ended.
    
        Returns:
            datetime: The calculated timestamp for the next session.
        """
        account_state = self.account_states[account_id]
        account_data_ext = self.account_map_data[account_id]
    
        # Retrieve base retention probability
        retention_probability = min(self.calculate_retention_probability(account_id, last_session_end), 0.75)
    
        # Define session gap parameters
        min_gap_hours = R.MIN_GAP_HOURS  # Minimum gap between sessions
        max_gap_hours = R.MAX_GAP_HOURS  # Maximum gap between sessions
        base_gap_hours = random.randint(min_gap_hours, max_gap_hours)
    
        # Fatigue penalty: More sessions today = larger gap
        sessions_today = account_state.get("sessions_today", 0)
        fatigue_penalty = min(0.25 * math.log1p(sessions_today), 0.4) if sessions_today >= 2 else 0.0
    
        # Enforce daily session cap (Players should not exceed 6 sessions per day)
        max_sessions_per_day = R.MAX_SESSIONS_PER_DAY
        if sessions_today >= max_sessions_per_day:
#            print(f"! User {account_id} has reached session limit ({sessions_today}/day). Delaying next session.")
            forced_days_forward = random.randint(1, 3)  # Delay by 1-3 days
            next_session_date = (last_session_end + timedelta(days=forced_days_forward)).replace(
                hour=random.randint(9, 22), minute=random.randint(0, 59)
            )
            account_state["sessions_today"] = 0  # Reset session count for the new day
            return next_session_date
    
        # Check day-of-week effect (higher consecutive play chance on weekends)
        weekday = last_session_end.weekday()
        weekend_boost = 0.7 if weekday in [4, 5, 6] else 1.0  # Boost retention on Fri-Sun
    
        # Check hour-of-day effect (higher consecutive play during peak hours)
        hour_of_day = last_session_end.hour
        peak_hour_boost = 0.8 if 12 <= hour_of_day <= 13 or 19 <= hour_of_day <= 23 else 1.0
    
        # Adjust session gap with all factors included
        adjusted_gap_hours = base_gap_hours * (1 - retention_probability) + fatigue_penalty
        adjusted_gap_hours *= weekend_boost * peak_hour_boost  # Apply engagement boost
        adjusted_gap = timedelta(hours=max(min_gap_hours, adjusted_gap_hours))  # Ensure min gap is respected
    
        # Determine next session time
        next_session_date = last_session_end + adjusted_gap
    
        # Decide whether to move session to the next day or beyond
        if next_session_date.date() == last_session_end.date():
            account_state["sessions_today"] += 1  # Increment session count
        else:
            account_state["sessions_today"] = 1  # Reset for a new day
    
        return next_session_date

    def update_final_account_state(self, account_id, new_state):
        """
        Update the final account state for a given account_id. If it exists, replace the state; otherwise, add it.
    
        Args:
            account_id (str): The unique identifier for the account.
            new_state (dict): The new account state to update.
        """
        # Check if the account already exists in final_account_states
        for account in self.final_account_states:
            if account["account_id"] == account_id:
                # Replace the existing state
                account.update(new_state)
                return
    
        # If account_id is not found, append the new state
        self.final_account_states.append(new_state)

    def assign_ab_test_version(self, account_id, device_model, base_timestamp, account_map_data):
        """
        Assign app_version 1.0.0.b to accounts based on stratified daily rollout by device model.
        Also: after AB_TEST_END_DATE, revert any AB version back to CONTROL_VERSION exactly once.
        """
        day = base_timestamp.date()

        if day < R.AB_TEST_LAUNCH_DATE.date():
            return

        if day > R.AB_TEST_END_DATE.date():
            if (account_id not in self.reverted_ab_accounts
                    and account_map_data.get("app_version") == R.AB_TEST_VERSION):
                account_map_data["app_version"] = R.CONTROL_VERSION
                self.reverted_ab_accounts.add(account_id)
            return

        # 3) Within test window: only assign once per account
        if account_id in self.processed_ab_accounts:
            return

        self.processed_ab_accounts.add(account_id)

        threshold = R.AB_TEST_DAILY_THRESHOLDS.get(day, R.AB_TEST_TARGET_PERCENTAGE)

        self.ab_assignment_device_counter[device_model]['total'] += 1
        total = self.ab_assignment_device_counter[device_model]['total']
        test_count = sum(self.ab_assignment_device_counter[device_model]['test'].values())

        if test_count / total < threshold:
            account_map_data["app_version"] = R.AB_TEST_VERSION
            self.ab_assignment_device_counter[device_model]['test'][day] += 1
        else:
            account_map_data["app_version"] = R.CONTROL_VERSION

    def generate_tutorial_events(self, account_id, session_id, start_timestamp):
        base_timestamp = start_timestamp
        account_state = self.account_states[account_id]
        account_data_ext = self.account_map_data[account_id]
        # Add user_login event if not initialized
        if not account_state.get("session_initialized", False):
            user_login_event = self.event_handler.write_event(
                event_type="authentication",
                event_subtype="user_login",
                event_date=base_timestamp,
                account_id=account_id,
                session_id=session_id,
                device_model=account_data_ext["device_model"],
                os_version=account_data_ext["os_version"],
                app_version=account_data_ext["app_version"],
            )
            self.events.append(user_login_event)
            account_state["session_initialized"] = True
        
        for tutorial_key, tutorial_level in self.tutorial_levels.items():
            equipped_hero = random.choice([hero for hero, owned in account_state["equipment"]["heroes"].items() if owned == 1]) if any(
                owned == 1 for owned in account_state["equipment"]["heroes"].values()) else "None"
            equipped_weapon = random.choice([weapon for weapon, count in account_state["equipment"]["weapons"].items() if count > 0]) if any(
                count > 0 for count in account_state["equipment"]["weapons"].values()) else ""
            equipped_held_item = random.choice([item for item, count in account_state["equipment"]["held_items"].items() if count > 0]) if any(
                count > 0 for count in account_state["equipment"]["held_items"].values()) else ""
            equipped_armor = random.choice([armor for armor, count in account_state["equipment"]["armor"].items() if count > 0]) if any(
                count > 0 for count in account_state["equipment"]["armor"].values()) else ""
            
            base_timestamp += timedelta(seconds=random.randint(5, 15))
            level_start_event = self.event_handler.write_event(
                event_type="progression",
                event_subtype="level_start",
                event_date=base_timestamp,
                account_id=account_id,
                session_id=session_id,
                level_id=tutorial_key,
                equipped_hero=equipped_hero,
                equipped_weapon=equipped_weapon,
                equipped_held_item=equipped_held_item,
                equipped_armor=equipped_armor
            )
            self.events.append(level_start_event)

            # Simulate level success
            time_spent = random.randint(tutorial_level["time"] // 2, tutorial_level["time"] - 1)
            total_score = random.randint(tutorial_level["success_score_floor"], tutorial_level["three_stars_floor"])
            stars_gained = (
                3 if total_score >= tutorial_level["three_stars_floor"] else
                2 if total_score >= tutorial_level["two_stars_floor"] else 1
            )
            base_timestamp += timedelta(seconds=time_spent)

            level_success_event = self.event_handler.write_event(
                event_type="progression",
                event_subtype="level_success",
                event_date=base_timestamp,
                account_id=account_id,
                session_id=session_id,
                level_id=tutorial_key,
                equipped_hero=equipped_hero,
                equipped_weapon=equipped_weapon,
                equipped_held_item=equipped_held_item,
                equipped_armor=equipped_armor,
                time=time_spent,
                total_score=total_score,
                stars_gained=stars_gained
            )
            self.events.append(level_success_event)

            # Process Rewards
            for item_category, item_id, item_amount in zip(
                tutorial_level["item_category"], tutorial_level["item_id"], tutorial_level["item_amount"]
            ):
                if callable(item_id):
                    item_id = item_id()
                if item_category == "equipment":
                    subcategory_map = C.item_subcategory_map
                    item_prefix = item_id.split("_")[1]
                    item_category = subcategory_map.get(item_prefix)
                    if not item_category:
                        raise ValueError(f"Unexpected equipment prefix: {item_prefix}")
                base_timestamp += timedelta(seconds=1)
                # Append reward event and update account state
                reward_event = self.event_handler.write_event(
                    event_type="resource",
                    event_subtype="source_item",
                    event_date=base_timestamp,
                    account_id=account_id,
                    session_id=session_id,
                    item_category=item_category,
                    item_id=item_id,
                    item_amount=item_amount,
                    reason=tutorial_key
                )
                self.events.append(reward_event)
            
                # Update account state
                if item_category in ["weapons", "held_items", "armor"]:
                    account_state["equipment"][item_category][item_id] = (
                        account_state["equipment"][item_category].get(item_id, 0) + item_amount
                    )
                elif item_category in ["heroes", "skins"]:
                    account_state["equipment"][item_category][item_id] = 1
                elif item_category == "currency":
                    if item_id == "currency_gold":
                        account_state["total_gold"] += item_amount
                    elif item_id == "currency_diamond":
                        account_state["total_diamond"] += item_amount
            # Increment timestamp for next tutorial level
            base_timestamp += timedelta(seconds=random.randint(5, 15))
#        print(f"Timestamp at the end of tutorial for {account_id}: {base_timestamp}")
#        print(f"Timestamp at the start of tutorial for {account_id}: {start_timestamp}")
        return base_timestamp, start_timestamp

    def generate_primary_loop(self, account_id, session_id, start_timestamp, tutorial_timestamp):
#        print(f"Start timestamp at the start of the session for {account_id}: {start_timestamp}")
        base_timestamp = start_timestamp
        account_state = self.account_states[account_id]
        account_state["recent_engagement_event"] = None
        account_data_ext = self.account_map_data[account_id]
        player_skill_interval = account_data_ext.get("player_skill", (1.0, 1.0))  # Default to (1.0, 1.0) if not present
        player_skill = random.uniform(*player_skill_interval)  # Pick a single value from the interval
        ad_engagement_chance = account_data_ext.get("ad_engagement_probability", R.DEFAULT_AD_ENGAGEMENT_PROB)
        current_level = account_state["last_completed_level"] + 1 if account_state["last_completed_level"] else 1
        archetype_key = account_data_ext["archetype"]
        terminate_session = False  # Track if the session should terminate
        eh = self.event_handler  # local ref to avoid attribute lookups
        emit = eh.make_emitter(
            account_id=account_id,
            session_id=session_id,
            device_model=account_data_ext["device_model"],
            os_version=account_data_ext["os_version"],
            app_version=account_data_ext["app_version"],
            currency_name=account_data_ext["currency_name"],
            exchange_rate=account_data_ext["exchange_rate"],
        )

        while current_level <= len(self.level_data):
            start_timestamp_fix = tutorial_timestamp if tutorial_timestamp else start_timestamp
            if terminate_session:
                print(f"?User {account_id} started its loop with terminate_session, session ended at the start...")
                account_state["session_initialized"] = False
                account_state["last_session_date"] = base_timestamp
                self.update_final_account_state(account_id, account_state)
                break  # Exit the loop if the session is terminated
            level_key = f"LEVEL_{current_level:03d}"
            if level_key not in self.level_data:
                print(f"Level ID {level_key} does not exist in level_data. Ending level progression for account {account_id}.")
                break
                
            # Check if the subscription has expired
            if account_state.get("subscription_date") is not None:
                subscription_end_date = account_state["subscription_date"] + timedelta(days=R.SUBSCRIPTION_DURATION_DAYS)
                if subscription_end_date <= base_timestamp:
                    account_state["subscription_date"] = None
                    account_state["is_subscribed"] = None  # Reset subscription status
                    
            self.assign_ab_test_version(account_id, account_data_ext["device_model"], base_timestamp, account_data_ext)
            level_cfg = self.level_data[level_key]

            if account_state.pop("__gear_dirty__", False):
                self._mark_gear_dirty(account_id)
            self.determine_equipment(account_id)
            success_chance = round(self.calculate_success_chance(account_id, level_cfg, current_level), 2)
            equipped_hero = account_state["equipped_items"]["equipped_hero"]
            equipped_skin = account_state["equipped_items"]["equipped_skin"]
            equipped_weapon = account_state["equipped_items"]["equipped_weapon"]
            equipped_held_item = account_state["equipped_items"]["equipped_held_item"]
            equipped_armor = account_state["equipped_items"]["equipped_armor"]

            # Add user_login event if not initialized
            if not account_state.get("session_initialized", False):
                user_login_event = emit(
                    event_type="authentication",
                    event_subtype="user_login",
                    event_date=base_timestamp,
                )
                self.events.append(user_login_event)
                account_state["session_initialized"] = True
                
            base_timestamp += timedelta(seconds=random.randint(1, 5))

            if account_data_ext["referred_friend"] and account_data_ext["referral_timestamp"] < base_timestamp:
                ref_code = account_data_ext["referral_code"]
                # Write the referral reward event
                reward_event = emit(
                    event_type="resource",
                    event_subtype="source_item",
                    event_date=base_timestamp,
                    item_category="chests",
                    item_id=R.REFERRAL_REWARD_CHEST_ID,
                    item_amount=1,
                    reason=f"Friend referral: {ref_code}"  # Store referral code as reason
                )
                
                # Append the event to the events list
                self.events.append(reward_event)
                
                # Prevent duplicate rewards by resetting the flag
                account_data_ext["referred_friend"] = False

            base_timestamp += timedelta(seconds=random.randint(1, 5))
            
            # Level Start Event
            level_start_event = emit(
                event_type="progression",
                event_subtype="level_start",
                event_date=base_timestamp,
                level_id=level_key,
                equipped_hero=equipped_hero,
                equipped_skin=equipped_skin,
                equipped_weapon=equipped_weapon,
                equipped_held_item=equipped_held_item,
                equipped_armor=equipped_armor
            )
            level_start_event, terminate_session = self.error_generator.attempt_event_replacement(
                level_start_event, account_data_ext, self.events, start_timestamp_fix, emit
            )
            if terminate_session:
#                print(f"{account_id}: Session terminated after level_start_event at {base_timestamp}")
                account_state["session_initialized"] = False
                account_state["last_session_date"] = base_timestamp
                self.update_final_account_state(account_id, account_state)
                break
            if level_start_event:
                self.events.append(level_start_event)
    
            # Determine level outcome
            random_success = round(random.random() / R.SUCCESS_RANDOM_DIVISOR, 2)
#            print(f"Within primary loop for {account_id}:") # Debug code
#            print(f"- Success chance: {success_chance}") # Debug code
#            print(f"- Random success check to surpass: {random_success}") # Debug code
            if random_success < success_chance:
#                print("Success event") # Debug code
                time_spent = level_cfg["time"]
                score_factor = round(success_chance - random_success, 4)
                base_score = level_cfg["success_score_floor"]
                max_score = level_cfg["three_stars_floor"] * R.MAX_SCORE_MULTIPLIER
                
                # Scale the score factor within the defined range
                total_score = base_score + (score_factor * (max_score - base_score))
                total_score = max(base_score, min(total_score, max_score))  # Clamp values
#                print(f"Total score= {total_score}, base score= {base_score}, max score= {max_score}, score factor= {score_factor}")
                stars_gained = (
                    3 if total_score >= level_cfg["three_stars_floor"] else
                    2 if total_score >= level_cfg["two_stars_floor"] else 1
                )
                base_timestamp += timedelta(seconds=time_spent)
            
                # Level Success Event
                level_success_event = emit(
                    event_type="progression",
                    event_subtype="level_success",
                    event_date=base_timestamp,
                    level_id=level_key,
                    equipped_hero=equipped_hero,
                    equipped_skin=equipped_skin,
                    equipped_weapon=equipped_weapon,
                    equipped_held_item=equipped_held_item,
                    equipped_armor=equipped_armor,
                    time=time_spent,
                    total_score=total_score,
                    stars_gained=stars_gained
                )
                level_success_event, terminate_session = self.error_generator.attempt_event_replacement(
                    level_success_event, account_data_ext, self.events, start_timestamp_fix, emit
                )
                base_timestamp += timedelta(seconds=1)
                if terminate_session:
#                    print(f"{account_id}: Session terminated after level_success_event at {base_timestamp}")
                    account_state["session_initialized"] = False
                    account_state["last_session_date"] = base_timestamp
                    self.update_final_account_state(account_id, account_state)
                    break
                if level_success_event:
                    # Increment level and update account state
                    account_state["last_completed_level"] = current_level
                    account_state["total_wins"] += 1
                    account_state["success_streak"] = account_state.get("success_streak", 0) + 1
                    account_state["failure_streak"] = 0
                    current_level += 1
                    self.events.append(level_success_event)
                    self.analytics.log_level_outcome(level_key, outcome=True)
            
                base_timestamp += timedelta(seconds=1)
            
                # Handle Rewards
                item_category = level_cfg["item_category"][0]
                item_id = level_cfg["item_id"][0]
                item_amount = level_cfg["item_amount"][0]
                
                reward_event = emit(
                    event_type="resource",
                    event_subtype="source_item",
                    event_date=base_timestamp,
                    item_category=item_category,
                    item_id=item_id,
                    item_amount=item_amount,
                    reason=level_key
                )
                reward_event, terminate_session = self.error_generator.attempt_event_replacement(
                    reward_event, account_data_ext, self.events, start_timestamp_fix, emit
                )
                if terminate_session:
#                    print(f"{account_id}: Session terminated after reward_event at {base_timestamp}")
                    account_state["session_initialized"] = False
                    account_state["last_session_date"] = base_timestamp
                    self.update_final_account_state(account_id, account_state)
                    break
                if reward_event:
                    self.events.append(reward_event)
                    if item_category == "currency":
                        if item_id == "currency_gold":
                            account_state["total_gold"] += item_amount
                        elif item_id == "currency_diamond":
                            account_state["total_diamond"] += item_amount
                    elif item_category == "chests":
                        self.chest_handler.add_chest_to_inventory(account_state, item_id, item_amount)
                        
                base_timestamp += timedelta(seconds=1)
                # Trigger Advertisement Event
                if account_state["is_subscribed"] == None:
                    ad_prob = self.ad_event_generator.ad_probability
                else:
                    ad_prob = 0
                if random.random() < ad_prob:
                    ad_data = self.ad_event_generator.select_ad()
                    ad_length = ad_data.get("ad_length", R.DEFAULT_AD_LENGTH)
                    terminate_session = self.ad_event_generator.create_ad_event(
                        event_date=base_timestamp,
                        event_subtype="ad_shown",
                        events=self.events,
                        account_map_data=account_data_ext,
                        start_timestamp_fix = start_timestamp_fix,
                        ad_data=ad_data,
                        emit=emit
                    )
                    if terminate_session:
#                        print(f"{account_id}: Session terminated after create ad_shown event at {base_timestamp}")
                        account_state["session_initialized"] = False
                        account_state["last_session_date"] = base_timestamp
                        self.update_final_account_state(account_id, account_state)
                        break  # Exit immediately on session termination
                
                    # Determine if the ad is skipped or completed
                    if random.random() < 1 - ad_engagement_chance:
                        # Generate the "ad_skipped" event
                        low = min(R.MIN_AD_WATCH_LENGTH, ad_length)
                        watched_seconds = random.randint(low, ad_length)
                        terminate_session = self.ad_event_generator.create_ad_event(
                            event_date=base_timestamp + timedelta(seconds=watched_seconds),
                            event_subtype="ad_skipped",
                            events=self.events,
                            account_map_data=account_data_ext,
                            start_timestamp_fix = start_timestamp_fix,
                            ad_data=ad_data,
                            emit=emit,
                            watched_seconds=watched_seconds,
                            remaining_seconds=ad_length - watched_seconds
                        )
                        if terminate_session:
#                            print(f"{account_id}: Session terminated after ad_skip event at {base_timestamp}")
                            account_state["session_initialized"] = False
                            account_state["recent_engagement_event"] = False
                            account_state["last_session_date"] = base_timestamp
                            self.update_final_account_state(account_id, account_state)
                            break  # Exit immediately on session termination
                
                        base_timestamp += timedelta(seconds=watched_seconds)  # Adjust timestamp
                    else:
                        # Generate the "ad_completed" event
                        terminate_session = self.ad_event_generator.create_ad_event(
                            event_date=base_timestamp + timedelta(seconds=ad_length),
                            event_subtype="ad_completed",
                            events=self.events,
                            account_map_data=account_data_ext,
                            start_timestamp_fix = start_timestamp_fix,
                            ad_data=ad_data,
                            emit=emit
                        )
                        if terminate_session:
#                            print(f"{account_id}: Session terminated after ad_completed event at {base_timestamp}")
                            account_state["session_initialized"] = False
                            account_state["last_session_date"] = base_timestamp
                            self.update_final_account_state(account_id, account_state)
                            break  # Exit immediately on session termination
                
                        base_timestamp += timedelta(seconds=ad_length)  # Adjust timestamp
                
                    # Update the ad probability based on whether an ad was shown
                    self.ad_event_generator.update_probability(True)
                else:
                    self.ad_event_generator.update_probability(False)

                base_timestamp += timedelta(seconds=1)
                # Attempt to open a chest
                available_chests = [chest_id for chest_id, count in account_state["chests"].items() if count > 0]
                if available_chests:
                    terminate_session, base_timestamp = self.chest_handler.open_all_chests(
                        account_state=account_state,
                        event_date=base_timestamp,
                        account_map_data=account_data_ext,
                        events=self.events,
                        start_timestamp_fix=start_timestamp_fix,
                        emit=emit
                    )
                    if terminate_session:  # Handle termination
#                        print(f"{account_id}: Session terminated after open_chest (level_success cond) event at {base_timestamp}")
                        account_state["session_initialized"] = False
                        account_state["last_session_date"] = base_timestamp
                        self.update_final_account_state(account_id, account_state)
                        break  # Exit immediately on session termination
                        
                base_timestamp += timedelta(seconds=random.randint(1, 5))
                
            else:  # Level Failed
#                print(f"Player {account_id} failed level {level_key}. SC: {success_chance}, RS: {random_success}") # Debug code
                self.analytics.log_level_outcome(level_key, outcome=False)
                time_spent = level_cfg["time"]
                fail_factor = round(random_success - success_chance, 4)
                min_score = level_cfg["success_score_floor"] * R.FAIL_SCORE_MIN_FACTOR
                max_score = level_cfg["success_score_floor"] * R.FAIL_SCORE_MAX_FACTOR
                
                # Scale the fail factor within the defined range
                total_score = max_score - (fail_factor * (max_score - min_score))
                total_score = max(min_score, min(total_score, max_score))  # Clamp values
#                print(f"Total score= {total_score}, min score= {min_score}, max score= {max_score}, fail factor= {fail_factor}")
                base_timestamp += timedelta(seconds=time_spent)
    
                level_fail_event = emit(
                    event_type="progression",
                    event_subtype="level_fail",
                    event_date=base_timestamp,
                    level_id=level_key,
                    equipped_hero=equipped_hero,
                    equipped_skin=equipped_skin,
                    equipped_weapon=equipped_weapon,
                    equipped_held_item=equipped_held_item,
                    equipped_armor=equipped_armor,
                    time=time_spent,
                    total_score=total_score
                )
                level_fail_event, terminate_session = self.error_generator.attempt_event_replacement(
                    level_fail_event, account_data_ext, self.events, start_timestamp_fix, emit
                )
                if terminate_session:
#                    print(f"{account_id}: Session terminated after level_fail at {base_timestamp}")
                    account_state["session_initialized"] = False
                    account_state["last_session_date"] = base_timestamp
                    self.update_final_account_state(account_id, account_state)
                    break
                if level_fail_event:
                    account_state["total_losses"] += 1
                    account_state["failure_streak"] = account_state.get("failure_streak", 0) + 1
                    account_state["success_streak"] = 0
                    self.events.append(level_fail_event)
                
                base_timestamp += timedelta(seconds=random.randint(1, 5))
                if random.random() < self.ad_event_generator.ad_probability:  # Use the dynamic ad probability
                    reward_ad_acceptance_probability = account_data_ext.get("reward_ad_acceptance_probability", 0.5)
                    # Generate reward ad events
                    terminate_session, ad_duration = self.ad_event_generator.create_reward_ad_event(
                        event_date=base_timestamp,
                        reward_ad_probability=reward_ad_acceptance_probability,
                        events=self.events,
                        account_state=account_state,
                        account_map_data=account_data_ext,
                        start_timestamp_fix=start_timestamp_fix,
                        emit=emit
                    )
                    if terminate_session:  # If an error event requires session termination
#                        print(f"{account_id}: Session terminated after reward_ad event at {base_timestamp}")
                        account_state["session_initialized"] = False
                        account_state["last_session_date"] = base_timestamp
                        self.update_final_account_state(account_id, account_state)
                        break
                    if ad_duration:  # Adjust the base timestamp if the ad was shown
                        base_timestamp += timedelta(seconds=ad_duration)
                        self.ad_event_generator.update_probability(True)  # Reset the ad probability
                    else:
                        self.ad_event_generator.update_probability(False)  # Increase ad probability
                        
            base_timestamp += timedelta(seconds=random.randint(5, 15))
            terminate_session, base_timestamp = self.business_event_generator.generate_business_event(
                account_id=account_id,
                session_id=session_id,
                event_date=base_timestamp,
                archetype_data=account_data_ext,
                events=self.events,
                account_state=account_state,
                account_map_data=account_data_ext,
                start_timestamp_fix=start_timestamp_fix,
                emit=emit
            )
            if terminate_session:
#                print(f"{account_id}: Session terminated after business event at {base_timestamp}")
                account_state["session_initialized"] = False
                account_state["last_session_date"] = base_timestamp
                self.update_final_account_state(account_id, account_state)
                break
                
            base_timestamp += timedelta(seconds=random.randint(1, 5))
            terminate_session, base_timestamp = self.in_game_purchase_generator.combine_items(
                account_state, base_timestamp, account_data_ext, self.events, start_timestamp_fix, emit
            )
            if terminate_session:  # Handle termination
#                print(f"{account_id}: Session terminated after combine_item event at {base_timestamp}")
                account_state["session_initialized"] = False
                account_state["last_session_date"] = base_timestamp
                self.update_final_account_state(account_id, account_state)
                break  # Exit immediately on session termination
                
            base_timestamp += timedelta(seconds=random.randint(1, 3))

            if random.random() < self.calculate_shop_activity_probability(account_id):
                self.analytics.log_shop_activity(archetype_key)
                base_timestamp, purchase_events, terminate_session = self.in_game_purchase_generator.generate_in_game_purchase_event(
                    base_timestamp, account_state, account_data_ext, self.events, start_timestamp_fix, emit
                )
                if terminate_session:
#                    print(f"{account_id}: Session terminated after in_game_purchase event at {base_timestamp}")
                    account_state["session_initialized"] = False
                    account_state["last_session_date"] = base_timestamp
                    self.update_final_account_state(account_id, account_state)
#                    print(f"Analysis: {account_id} broke out of session due to shop activity error.")
                    break  # End session
                self.events.extend(purchase_events)

            
#            if tutorial_timestamp:
#                print(f"Tutorial timestamp for account {account_id}, session {session_id}")
            session_duration = (base_timestamp - start_timestamp_fix).total_seconds() / 60  # Calculate duration in minutes
            session_termination_probability = self.calculate_session_termination_probability(
                account_id=account_id,
                session_duration=session_duration,
                success_streak=account_state.get("success_streak", 0),
                failure_streak=account_state.get("failure_streak", 0),
                current_timestamp=base_timestamp,
                recent_engagement_event=account_state.get("recent_engagement_event", None)
            )

            base_timestamp += timedelta(seconds=random.randint(5, 15))
            # Attempt to open a chest
            available_chests = [chest_id for chest_id, count in account_state["chests"].items() if count > 0]
            if available_chests:
                terminate_session, base_timestamp = self.chest_handler.open_all_chests(
                    account_state=account_state,
                    event_date=base_timestamp,
                    account_map_data=account_data_ext,
                    events=self.events,
                    start_timestamp_fix=start_timestamp_fix,
                    emit=emit
                )
                if terminate_session:  # Handle termination
#                    print(f"{account_id}: Session terminated after open_chest (level_fail cond) event at {base_timestamp}")
                    account_state["session_initialized"] = False
                    account_state["last_session_date"] = base_timestamp
                    self.update_final_account_state(account_id, account_state)
                    break  # Exit immediately on session termination
                
            base_timestamp += timedelta(seconds=random.randint(2, 5))
            stp_check = round(random.random(), 2)
            if stp_check < session_termination_probability or account_state["last_completed_level"] == 100:  # End session
#                print(f"+ SESSION TERMINATION EVENT: Session ended for account {account_id} at level {current_level-1} (STP= {session_termination_probability}, Check= {stp_check}, Time= {base_timestamp}).")
                self.update_final_account_state(account_id, account_state)
                # Add user_logout event at the end of the session
                user_logout_event = emit(
                    event_type="authentication",
                    event_subtype="user_logout",
                    event_date=base_timestamp,
                    session_duration=(base_timestamp - start_timestamp_fix).total_seconds()
                )
#                print(f"Base timestamp for {account_id} in session {session_id}: {base_timestamp}")
#                print(f"Start timestamp for {account_id} in session {session_id}: {start_timestamp}")
                self.events.append(user_logout_event)
                account_state["session_initialized"] = False
                account_state["last_session_date"] = base_timestamp
                return True  # Indicate session ended
    
        return False

    def generate_events_for_account(self, account_id, session_id, start_timestamp, tutorial_timestamp):
        """
        Generate events for a single session and return the last event's timestamp.
    
        Args:
            account_id (str): The account ID.
            session_id (int): The session ID.
            start_timestamp (datetime): The timestamp for the session start.
            tutorial_timestamp (datetime): The timestamp for the tutorial event.
    
        Returns:
            datetime: The timestamp of the last generated event.
        """
        # Record the current length of self.events before generating new events
        initial_event_count = len(self.events)
#        print(f"Length of events before generate_primary_loop: {initial_event_count}")
        # Generate events normally
        self.generate_primary_loop(account_id, session_id, start_timestamp, tutorial_timestamp)
#        print(f"Length of events after generate_primary_loop: {initial_event_count}")
    
        # Get newly generated events using list slicing
        # Since events are appended sequentially, this captures only the new events
        account_events = list(itertools.islice(self.events, initial_event_count, len(self.events)))
        
        if not account_events:
#            print(f"Debugging No Events Found:")
#            print(f"- Account ID: {account_id}")
#            print(f"- initial_event_count: {initial_event_count}")
#            print(f"- len(self.events): {len(self.events)}")
#            print(f"- Last Event Before Session: {self.events[initial_event_count-1] if initial_event_count > 0 else 'No previous events'}")
#            print(f"- Last Event in Dataset: {self.events[-1]}")
            
            raise ValueError(f"Account {account_id} generated no events.")
    
        # Get the last event's timestamp
        last_event_timestamp = account_events[-1]["event_date"]
        return last_event_timestamp

    def generate_multiple_sessions(self, account_id):
        """
        Generate multiple logically connected sessions for an account, starting from their signup date.
    
        Args:
            account_id (str): The unique identifier for the account.
    
        Returns:
            None: Updates the events dictionary in place.
        """
        # Initialize account state if not already done
        self.initialize_account_state(account_id)
#       loop_count = 0
        start_timestamp = None
    
        # Get signup date and add a short delay for first activity
        account_state = self.account_states[account_id]
        signup_date = self.account_map_data[account_id]["signup_date"]
        archetype_data = self.account_map_data[account_id]['archetype']
        account_state["account_archetype"] = archetype_data
        initial_timestamp = signup_date + timedelta(seconds=random.randint(30, 45))
        if account_state["full_churn"] == None:
            account_state["full_churn"] = self.account_map_data[account_id].get("full_churn", "ERROR")
            account_state["churn_log"].append(account_state["full_churn"])
    
        # Generate tutorial events once for the account
        start_timestamp, tutorial_timestamp = self.generate_tutorial_events(account_id, session_id=1, start_timestamp=initial_timestamp)
        tutorial_state = True
        # Prepare for multiple sessions
        start_timestamp += timedelta(seconds=random.randint(5, 10))  # Slight gap after tutorials
        session_id = 2
#       last_level = account_state["last_completed_level"]
#       print(f"Last level for {account_id}: {last_level}")
    
        # Generate sessions in a loop
        while start_timestamp.date() <= R.END_DATE.date():
            # Determine the current day relative to signup
            current_day = (start_timestamp.date() - signup_date.date()).days
        
            # Check for completion of all levels **before scheduling the session**
            if account_state["last_completed_level"] == 100:
#                print(f"$ COMPLETION EVENT: User {account_id} reached level 100. The loop is broken and the user is force-churned.")
                self.update_final_account_state(account_id, account_state)
                account_state["churned"] = "Completion"
                break  # Exit session loop
        
            # Proceed with session generation only if the player has not reached level 100
            if account_state.get("last_retention_day") != current_day:
                account_state["last_retention_day"] = current_day
        
            # Generate events for the session and get the last timestamp
            if tutorial_state:
                end_timestamp = self.generate_events_for_account(
                    account_id=account_id,
                    session_id=session_id,
                    start_timestamp=start_timestamp,
                    tutorial_timestamp=tutorial_timestamp
                )
            else:
                end_timestamp = self.generate_events_for_account(
                    account_id=account_id,
                    session_id=session_id,
                    start_timestamp=start_timestamp,
                    tutorial_timestamp=None
                )
        
            # Increment session ID and calculate the gap to the next session
            session_id += 1
            # loop_count += 1
            tutorial_state = False
            
            # Determine when the next session should start
            start_timestamp = self.calculate_return_date(account_id, end_timestamp)
            if start_timestamp > R.END_DATE:
#               final_churn = account_state["full_churn"]
                self.update_final_account_state(account_id, account_state)
                account_state["churned"] = "Timeout"
                break

            
            # Check retention probability
            rp_check = round(random.random(), 2)
            retention_probability = self.calculate_retention_probability(account_id, start_timestamp)
            if rp_check > retention_probability:
                # Update full_churn value
                account_state["churn_count"] += 1
                self.calculate_full_churn(account_state, rp_check, retention_probability)
            
                # Check if player is fully churned
                if account_state["full_churn"] <= 0:
#                    print(f"* CHURN EVENT: User {account_id} is churned out (RP: {round(retention_probability, 2)}, Check= {rp_check}, Loop count= {loop_count})")
                    self.analytics.log_retention(day=current_day)
                    account_state["churned"] = "Churned"
                    self.update_final_account_state(account_id, account_state)
                    break

    def get_final_account_states(self):
        """Return the final account states."""
        return self.final_account_states
        
    def assign_session_ids(self):
        """
        Assign unique session_id values to each event in the 'events' deque, ensuring chronological order
        and proper session boundaries per account.
        """
        current_time = datetime.now().strftime("%H:%M")
        print(f"Assigning session IDs started at {current_time}")
        # Step 1: Sort events globally by date
        self.events = deque(sorted(self.events, key=lambda e: e["event_date"]))
    
        # Step 2: Initialize a global session counter
        global_session_id = 1
        active_sessions = {}  # Tracks active sessions by account_id
    
        # Step 3: Iterate through events and assign session IDs
        for event in self.events:
            account_id = event["account_id"]
    
            # Handle user_login: start a new session
            if event["event_type"] == "authentication" and event["event_subtype"] == "user_login":
                # Start a new session and assign a unique session ID
                active_sessions[account_id] = global_session_id
                event["session_id"] = global_session_id
                global_session_id += 1
    
            # Handle user_logout: end the current session
            elif event["event_type"] == "authentication" and event["event_subtype"] == "user_logout":
                if account_id in active_sessions:
                    # Assign the session ID to the event
                    event["session_id"] = active_sessions[account_id]
                    # Remove the session from active sessions
                    del active_sessions[account_id]
    
            # Handle other events: assign the current session ID
            else:
                if account_id in active_sessions:
                    event["session_id"] = active_sessions[account_id]
                else:
                    # If no active session, leave session_id as None
                    event["session_id"] = None
                    
        current_time = datetime.now().strftime("%H:%M")
        print(f"Assigning session IDs ended at {current_time}")
        
    def generate_all_events(self):
        """
        Generate events for all accounts in the dataset using account_map_data.
        """
        total_accounts = len(self.account_map_data)
        processed_accounts = 0
        last_printed_percent = 0
        
        for account_id in self.account_map_data.keys():
            self.generate_multiple_sessions(account_id)
            processed_accounts += 1
            
            # Calculate completion percentage
            completion_percentage = int((processed_accounts / total_accounts) * 100)
            
            # Print only if completion percentage is a multiple of 10 and hasn't been printed before
            if completion_percentage % 10 == 0 and completion_percentage != last_printed_percent:
                current_time = datetime.now().strftime("%H:%M")
                prefix = f"Core {self.worker_id}: " if self.worker_id else ""
                print(f"{prefix}Events: {completion_percentage}% complete at {current_time}")
                last_printed_percent = completion_percentage
    
        # Assign session IDs after all events are generated
        self.assign_session_ids()
        summaries = self.analytics.generate_summary()
    
        return self.get_events(), summaries

    def get_events(self):
        return self.events

    def generate_sessions(self):
        """
        Generate the 'sessions' deque based on the 'events' deque and assigned session IDs.
        """
        # Ensure events are sorted by session_id and then by date within each session
        self.events = deque(sorted(self.events, key=lambda e: (e["session_id"], e["event_date"])))
    
        sessions = deque()
        tracked_sessions = set()
    
        total_events = len(self.events)
        processed_events = 0
        last_printed_percent = 0
    
        session_events = []  # Moved here to be reset per session
    
        for event in self.events:
            processed_events += 1
            completion_percentage = int((processed_events / total_events) * 100)
    
            if completion_percentage % 10 == 0 and completion_percentage != last_printed_percent:
                current_time = datetime.now().strftime("%H:%M")
                prefix = f"Core {self.worker_id}: " if self.worker_id else ""
                print(f"{prefix}Sessions: {completion_percentage}% complete at {current_time}")
                last_printed_percent = completion_percentage
    
            session_id = event.get("session_id")
            account_id = event["account_id"]
    
            # Skip events with no session_id
            if session_id is None:
                continue
    
            # If this is a new session, reset session_events
            if session_id not in tracked_sessions:
                tracked_sessions.add(session_id)
                session_events = []  # Reset the event tracker for this session
    
                sessions.append({
                    "session_id": session_id,
                    "account_id": account_id,
                    "session_start": None,
                    "session_end": None,
                    "region": self.account_map_data[account_id].get("country", "Unknown"),
                    "platform": None,
                    "device_model": None,
                    "os_version": None,
                    "app_version": None,
                    "end_reason": "THIS IS AN ERROR, CHECK tracked_sessions",
                })
    
            # Append event to session_events
            session_events.append(event)
    
            # Update session_start and session_end
            if event["event_type"] == "authentication" and event["event_subtype"] == "user_login":
                sessions[-1]["session_start"] = event["event_date"]
                metadata = event.get("event_metadata", {})
                sessions[-1]["device_model"] = metadata.get("device_model", "Unknown")
                sessions[-1]["os_version"] = metadata.get("os_version", "Unknown")
                sessions[-1]["app_version"] = metadata.get("app_version", "unknown")
                device_model = metadata.get("device_model", "").lower()
                if device_model.startswith("iphone") or device_model.startswith("ios"):
                    sessions[-1]["platform"] = "iOS"
                else:
                    sessions[-1]["platform"] = "Android"
    
            elif event["event_type"] == "authentication" and event["event_subtype"] == "user_logout":
                sessions[-1]["session_end"] = event["event_date"]
    
                # Identify the last event before logout
                if len(session_events) > 1:
                    last_event = session_events[-2]  # Second-to-last event (before logout)
                    if last_event["event_type"] == "error":
                        sessions[-1]["end_reason"] = last_event.get("event_subtype", "error")
                    else:
                        sessions[-1]["end_reason"] = "player_exit"
                else:
                    sessions[-1]["end_reason"] = "player_exit"
    
        # Sort sessions by session_start for consistency
        sorted_sessions = sorted(sessions, key=lambda s: s["session_start"])
        return deque(sorted_sessions)