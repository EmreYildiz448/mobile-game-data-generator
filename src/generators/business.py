from datetime import timedelta
from collections import defaultdict, deque
import random

from src import catalogs as C
from src.settings import runtime as R

class BusinessEventGenerator:
    def __init__(self, shop_offers, chest_handler, event_handler, error_generator):
        """
        Initialize the BusinessEventGenerator.
        """
        self.shop_offers = shop_offers
        self.chest_handler = chest_handler
        self.event_handler = event_handler
        self.error_generator = error_generator

        # Track per-account offer refresh dates
        self.offer_refresh_dates = {}  # account_id -> {offer_name: last_purchase_date}

        # NEW: track per-account recent purchases in a 24h rolling window
        self._recent_purchases = defaultdict(lambda: deque())  # account_id -> deque[(ts, offer_name)]
        self._recent_window = timedelta(days=1)

    def is_offer_available(self, offer_name, current_date, account_state, account_id):
        if account_id not in self.offer_refresh_dates:
            self.offer_refresh_dates[account_id] = {}
    
        last_purchase = self.offer_refresh_dates[account_id].get(offer_name)
        offer_data = self.shop_offers[offer_name]
    
        # Prevent purchasing hero bundles if the player already owns the hero
        if offer_name.startswith("off_hero_bundle"):  
            hero_id = offer_data["item_id"][0]  # First reward is always the hero
            if hero_id in account_state["equipment"].get("heroes", []):
                return False  
    
        if offer_name == "off_subscription_basic" or offer_name == "off_subscription_premium":
            # Use subscription_date instead of last_purchase
            subscription_start = account_state.get("subscription_date")
            
            # Subscription is available if the user has never subscribed OR the subscription has expired
            return subscription_start is None or (current_date - subscription_start).days >= R.SUBSCRIPTION_DURATION_DAYS

        elif "hero_bundle" in offer_name:
            return last_purchase is None or (current_date - last_purchase).days >= 7

#        elif offer_name == "off_item_bundle_epic":
#            return last_purchase is None or (current_date - last_purchase).days >= 1

        return True

    def select_offer(self, eligible_offers, archetype_key, recent_purchases):
        offer_names = list(eligible_offers.keys())
    
        # Base Weights for Offers
        base_weights = C.base_offer_weights
    
        # Archetype Modifiers with Subscription Adjustments
        archetype_modifiers = C.archetype_offer_modifiers
    
        modifiers = archetype_modifiers.get(archetype_key, {})
        weights = [
            base_weights.get(offer, 1.0) *
            modifiers.get(offer, 1.0) *
            (0.5 if offer in recent_purchases else 1.0)
            for offer in offer_names
        ]
    
        return random.choices(offer_names, weights=weights, k=1)[0]

    def generate_business_event(self, account_id, session_id, event_date, archetype_data, events, account_state, account_map_data, start_timestamp_fix, emit):
        """
        Generate a business event, apply rewards, and inject errors if applicable.
    
        Args:
            account_id (str): The account ID.
            session_id (str): The session ID.
            current_date (datetime): The current date and time.
            archetype_data (dict): Archetype-specific data.
            events (list): The list of session events.
            account_state (dict): The player's account state.
            account_map_data (dict): Metadata for the account.
    
        Returns:
            tuple: (terminate_session)
                - terminate_session (bool): Whether the session should terminate due to an error.
        """
        if account_id not in self.offer_refresh_dates:
            self.offer_refresh_dates[account_id] = {}

        def calculate_monetization_probability(account_map_data):
            """
            Calculate dynamic monetization probability for a specific account.
        
            Factors:
            - Base probability from archetype.
            - Account's spending level (free, low spender, high spender).
            - Stored currency, with free accounts unaffected.
        
            Returns:
                float: The calculated monetization probability.
            """
            account_data_ext = account_map_data
            archetype = account_data_ext.get("archetype", "")
            base_probability = account_data_ext.get("monetization_probability", 0.05)
        
            # Spending level adjustment
            if "free" in archetype:
                spending_factor = R.FREE_SPENDING_FACTOR
            elif "low_spender" in archetype:
                spending_factor = R.LOWSPENDER_SPENDING_FACTOR
            elif "high_spender" in archetype:
                spending_factor = R.HIGHSPENDER_SPENDING_FACTOR
            else:
                spending_factor = R.FALLBACK_SPENDING_FACTOR
        
            # Currency-based adjustment
#            total_gold = account_state.get("total_gold", 0)
#            total_diamonds = account_state.get("total_diamond", 0)
#            currency_factor = min((total_gold + total_diamonds) / 5000, 0.3)  # Max +30% for high currency
        
            # Total probability
            monetization_probability = base_probability + spending_factor # + currency_factor
            return min(monetization_probability, 1.0)
        
        monetization_probability = calculate_monetization_probability(account_map_data)
        monetization_check = random.random()
        if monetization_check > monetization_probability:
            return False, event_date  # No business event triggered
    
        eligible_offers = {
            name: offer for name, offer in self.shop_offers.items()
            if offer["cost_type"] == "money" and self.is_offer_available(name, event_date, account_state, account_id)
        }
    
        if not eligible_offers:
            return False, event_date  # No offers are available
    
        dq = self._recent_purchases[account_id]
        cutoff = event_date - self._recent_window
        while dq and dq[0][0] < cutoff:
            dq.popleft()

        recent_names = {name for (_, name) in dq}  # set of offer_name strings

        archetype_key = account_map_data["archetype"]
        offer_name = self.select_offer(eligible_offers, archetype_key, recent_names)
        offer = self.shop_offers[offer_name]
        
        raw_cost = offer["cost_amount"]
        exchange_rate = account_map_data.get("exchange_rate", 1.0)
        market_multiplier = account_map_data.get("market_multiplier", 1.0)
        currency_rounding = account_map_data.get("currency_rounding", 2)
        ab_monetization_effect = R.AB_MONETIZATION_EFFECT_TEST if account_map_data['app_version'] == R.AB_TEST_VERSION else R.AB_MONETIZATION_EFFECT_CONTROL
        
        # Apply both exchange rate and market-based adjustment
        adjusted_price = raw_cost * exchange_rate * market_multiplier * ab_monetization_effect
        cost_amount = round(adjusted_price, currency_rounding)
        
        # Generate business event
        business_event = emit(
            event_type="business",
            event_subtype="business",
            event_date=event_date,
            offer_id=offer["offer_id"],
            reward_category=offer["item_category"],
            reward_id=[item() if callable(item) else item for item in offer["item_id"]],
            reward_amount=offer["item_amount"],
            cost_type=offer["cost_type"],
            cost_amount=cost_amount
        )
        business_event, terminate_session = self.error_generator.attempt_event_replacement(
            business_event, account_map_data, events, start_timestamp_fix, emit
        )
        if terminate_session:
            return True, None  # Session terminated due to an error
        if business_event:
            events.append(business_event)
            account_state["total_money_spent"] += cost_amount

            self._recent_purchases[account_id].append((event_date, offer_name))
            self.offer_refresh_dates[account_id][offer_name] = event_date
            
            # Apply rewards
            event_date += timedelta(seconds=1)
            for category, reward_id, amount in zip(
                business_event["event_metadata"]["reward_category"],
                business_event["event_metadata"]["reward_id"],
                business_event["event_metadata"]["reward_amount"]
            ):
                # Generate source item event for rewards
                event_date += timedelta(seconds=1)
                source_event = emit(
                    event_type="resource",
                    event_subtype="source_item",
                    event_date=event_date,
                    item_category=category,
                    item_id=reward_id,
                    item_amount=amount,
                    reason=offer["offer_id"]
                )
                events.append(source_event)
                if category == "currency":
                    if reward_id == "currency_gold":
                        account_state["total_gold"] += amount
                    elif reward_id == "currency_diamond":
                        account_state["total_diamond"] += amount
                elif category == "chests":
                    self.chest_handler.add_chest_to_inventory(account_state, reward_id, amount)
                elif category in ['weapons', 'held_items', 'armor']:
                    account_state['equipment'][category][reward_id] = account_state['equipment'][category].setdefault(reward_id, 0) + 1
                    account_state["__gear_dirty__"] = True
                else:
                    account_state["equipment"][category][reward_id] = 1
                    account_state["__gear_dirty__"] = True
                        
            offer_split = offer_name.split('_')
            if offer_split[1] == "subscription":
                account_state["subscription_date"] = event_date
                if offer_split[2] == "basic":
                    account_state["is_subscribed"] = "basic"
                elif offer_split[2] == "premium":
                    account_state["is_subscribed"] = "premium"

        self.offer_refresh_dates[account_id][offer_name] = event_date
        archetype_data["monetization_probability"] = max(0.0, monetization_probability - 0.5)
        return False, event_date