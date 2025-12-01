import random
import math
import pandas as pd
from datetime import datetime, timedelta

from src.settings import runtime as R

class HostedAdGenerator:
    def __init__(self, seed=None):
        """
        Initialize the generator with an optional seed for reproducibility.
        """
        if seed is not None:
            random.seed(seed)
        self.seed = seed

        # Ad Networks
        self.ad_networks = ["Google AdMob", "Unity Ads", "Meta Audience Network", "ironSource", "AppLovin"]

        # Advertised Products
        self.advertised_products = [
            "Clash of Kingdoms", "Empire Reborn", "Age of Conquest", "Totally War: Frontiers",
            "Dominion Siege", "Battle for Supremacy", "Kingdoms Rising", "Warrior's Legacy",
            "Monster Battle Arena", "Mystic Legends", "Dark Rift Chronicles", "Eternal Odyssey",
            "Shadowborn Saga", "Celestial Knights", "Realm of Heroes", "The Arcane Order",
            "Space Colonizers", "Galaxy Warzone", "Alien Expanse", "Cybernetic Wars",
            "Mech Titan Clash", "Starship Outlaws", "Infinite Void", "Neon Star Crusaders",
            "Zombie Invasion", "Underground Fighters", "Post-Apocalyptic Havoc", "Rogue City Mayhem",
            "Urban Mercenaries", "Shadow Ops: Blackout", "The Last Survivor", "Gunfight Legends",
            "Tower Defense X", "Magic Quest", "Pixel Racers", "Brain Benders Deluxe",
            "Legendary Wordsmiths", "Candy Pop Adventures", "Escape Room Challenge", "Hidden Object Mysteries"
        ]

        # Shuffle and create a product iterator
        random.shuffle(self.advertised_products)
        self.product_iterator = iter(self.advertised_products)

        # Pricing model constraints
        self.pricing_models = ["CPC", "CPI", "CPM", "CPA"]

        # Adjusted weight distribution for more CPM ads
        self.pricing_weights = [1, 1, 3, 1]  # More CPM ads

        # **Updated Pricing Model Ranges (in USD)**
        self.pricing_ranges = {
            "CPC": (0.50, 3.00),
            "CPI": (2.00, 10.00),
            "CPM": (2.00, 10.00),
            "CPA": (5.00, 15.00),
        }

        self.start_date = R.START_DATE
        self.end_date = R.START_DATE + timedelta(days=365)  # All ads end within this timeframe

    def generate_value_per_action(self, pricing_model):
        """
        Generate a realistic value per action based on the pricing model.
        """
        min_price, max_price = self.pricing_ranges[pricing_model]
        value = random.uniform(min_price, max_price)
        return round(value * 4) / 4  # Round to nearest 0.25

    def get_next_product(self):
        """
        Retrieve the next unique advertised product from the iterator.
        If the iterator is exhausted, reshuffle the list and restart.
        """
        try:
            return next(self.product_iterator)
        except StopIteration:
            # Reshuffle and restart if we run out of unique product names
            random.shuffle(self.advertised_products)
            self.product_iterator = iter(self.advertised_products)
            return next(self.product_iterator)

    def generate_ads(self, num_ads=30):
        """
        Generate a set of hosted advertisements with a mix of pricing models and reward eligibility.
        """
        ads = []
        num_rewarded_ads = max(4, int(num_ads * 0.35))  # Ensure at least 4, target 30-40% as rewarded

        # **Step 1: Ensure at least one rewarded ad per pricing model**
        rewarded_ads = []
        for pricing_model in self.pricing_models:
            rewarded_ads.append({
                "ad_id": f"EXT_AD_{len(ads):03}",
                "ad_network": random.choice(self.ad_networks),
                "advertised_product": self.get_next_product(),
                "pricing_model": pricing_model,
                "value_per_action": self.generate_value_per_action(pricing_model) * 2,
                "start_date": self.start_date.date(),
                "end_date": self.end_date.date(),
                "is_active": True,
                "ad_length": random.choice([30, 45, 60]),
                "rewarded": True
            })
            ads.append(rewarded_ads[-1])

        # **Step 2: Generate Remaining Ads**
        while len(ads) < num_ads:
            pricing_model = random.choices(self.pricing_models, weights=self.pricing_weights)[0]
            is_rewarded = len(rewarded_ads) < num_rewarded_ads and random.random() < 0.35  # 35% chance

            ad = {
                "ad_id": f"EXT_AD_{len(ads):03}",
                "ad_network": random.choice(self.ad_networks),
                "advertised_product": self.get_next_product(),
                "pricing_model": pricing_model,
                "value_per_action": self.generate_value_per_action(pricing_model) * (2 if is_rewarded else 1),
                "start_date": self.start_date.date(),
                "end_date": self.end_date.date(),
                "is_active": True,
                "ad_length": random.choice([30, 45, 60]),
                "rewarded": is_rewarded
            }
            ads.append(ad)

            if is_rewarded:
                rewarded_ads.append(ad)

        return ads

    def generate_all(self):
        """
        Generate hosted advertisements.
        """
        return self.generate_ads()

class HostedAdInteractionGenerator:
    def __init__(self, ad_campaigns, events, player_archetypes, account_map_data):
        # Core data
        self.player_archetypes = player_archetypes

        # Campaign lookup → O(1)
        camp_df = pd.DataFrame(ad_campaigns)
        self.campaign_lookup = {
            row.ad_id: (row.pricing_model, float(row.value_per_action), bool(row.rewarded))
            for row in camp_df.itertuples(index=False)
        }

        # Accounts lookup → O(1)
        self.accounts_by_id = {a["account_id"]: a for a in account_map_data}

        # Keep only ad-shown events once (ad/rewarded-ad inventory)
        full_events_df = pd.DataFrame(events)
        ad_events_df = full_events_df[
            (full_events_df["event_type"] == "ad")
            & (full_events_df["event_subtype"].isin(["ad_shown", "reward_ad_shown"]))
        ][["account_id", "event_date", "event_subtype", "event_metadata"]].copy()

        # (A) Pre-split by account → avoids refiltering later
        self.events_by_account = {
            aid: df for aid, df in ad_events_df.groupby("account_id", sort=False)
        }

        # (B) Precompute exposures per (account_id, ad_id) → O(1) lookups later
        # Extract ad_id column (avoid .apply in hot paths)
        ad_events_df["__ad_id__"] = ad_events_df["event_metadata"].map(lambda x: x.get("ad_id"))
        grp = ad_events_df.groupby(["account_id", "__ad_id__"], sort=False).size()
        self.exposure_count = dict(grp.items())

        # CPI/CPA completion memory
        self.completed_interactions = set()  # set[(account_id, ad_id)]

    def _dynamic_probability(self, base_p: float, exposure_count: int, is_rewarded: bool, kind: str) -> float:
        # Diminishing returns from exposures
        exposure_effect = min(math.log1p(exposure_count) * 0.05, 0.2)
        # Rewarded boost
        reward_boost = 0.2 if is_rewarded or (kind in ("reward_ad_shown", "reward_ad_completed")) else 0.0
        # Action type modifier
        if kind in ("install", "action"):
            act = random.uniform(0.01, 0.05)
        else:
            act = random.uniform(0.05, 0.10)
        p = (base_p + exposure_effect + reward_boost) * act
        return min(p, 1.0)

    def _time_delay(self, pricing_model: str) -> timedelta:
        if pricing_model == "CPM":
            return timedelta(seconds=random.randint(1, 10))
        if pricing_model == "CPC":
            return timedelta(seconds=random.randint(5, 60))
        if pricing_model == "CPI":
            return timedelta(seconds=random.randint(300, 7200))
        if pricing_model == "CPA":
            return timedelta(seconds=random.randint(1800, 86400))
        return timedelta(0)

    def generate_interactions_for_account(self, account_id: int):
        interactions = []

        # Early outs if this account had no ad events
        account_events = self.events_by_account.get(account_id)
        if account_events is None or account_events.empty:
            return interactions

        acc = self.accounts_by_id[account_id]
        archetype = acc["archetype"]
        device_model = acc["device_model"]
        platform = "iOS" if device_model.lower().startswith(("iphone", "ios")) else "Android"
        base_p = self.player_archetypes[archetype]["ad_engagement_probability"]

        # Iterate quickly
        for row in account_events.itertuples(index=False):
            # Pull ad_id without lambda/apply
            ad_id = row.event_metadata.get("ad_id")
            if not ad_id or ad_id == "None":
                continue

            # Campaign info in O(1)
            camp = self.campaign_lookup.get(ad_id)
            if camp is None:
                continue
            pricing_model, vpa, is_rewarded = camp

            # Exposure count in O(1)
            exp_cnt = self.exposure_count.get((account_id, ad_id), 0)

            # Decide interaction
            if pricing_model == "CPM":
                interaction_type = "view"
                revenue = vpa / 1000.0
                dt = self._time_delay(pricing_model)

            elif pricing_model == "CPC":
                p = self._dynamic_probability(base_p, exp_cnt, is_rewarded, "click")
                if random.random() >= p:
                    continue
                interaction_type = "click"
                revenue = vpa
                dt = self._time_delay(pricing_model)

            elif pricing_model in ("CPI", "CPA"):
                kind = "install" if pricing_model == "CPI" else "action"
                p = self._dynamic_probability(base_p, exp_cnt, is_rewarded, kind)
                if random.random() >= p:
                    continue

                # one-time per (account, ad)
                key = (account_id, ad_id)
                if key in self.completed_interactions:
                    continue
                self.completed_interactions.add(key)

                interaction_type = kind
                revenue = vpa
                dt = self._time_delay(pricing_model)

            else:
                continue  # unknown model

            interactions.append({
                "account_id": account_id,
                "ad_id": ad_id,
                "interaction_time": row.event_date + dt,
                "interaction_type": interaction_type,
                "revenue": float(revenue),
                "platform": platform,
                "region": acc["country"],
                "device_model": device_model,
            })

        return interactions

    def generate_all_interactions(self):
        all_interactions = []
        for account_id in self.accounts_by_id.keys():
            all_interactions.extend(self.generate_interactions_for_account(account_id))
        all_interactions.sort(key=lambda x: x["interaction_time"])
        return all_interactions