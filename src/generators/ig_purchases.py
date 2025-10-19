import random
from datetime import timedelta

from src import catalogs as C
from src.settings import runtime as R

class InGamePurchaseGenerator:
    def __init__(self, shop_offers, item_data, chest_handler, event_handler, error_generator, analytics, seed=None):
        """
        Initialize the InGamePurchaseGenerator with shop offers, item data, an EventHandler instance, and an ErrorGenerator.
        """
        self.shop_offers = shop_offers
        self.item_data = item_data
        self.chest_handler = chest_handler
        self.event_handler = event_handler
        self.error_generator = error_generator
        self.analytics = analytics  # Initialize the AnalyticsFramework
        self.seed = seed
        if seed is not None:
            random.seed(seed)

    def filter_offers(self, account_state, account_map_data):
        """
        Filter eligible offers based on account state and shop data, with special logic for skins and diamond offers.
        """
        eligible_offers = {}
        for offer_name, offer_data in self.shop_offers.items():
            # Skip money-based offers
            if offer_data["cost_type"] == "money":
                continue
            
            # Handle diamond offers with saving logic
            if offer_data["cost_type"] == "currency_diamond":
                diamond_threshold = account_map_data.get("diamond_upper_limit", 50)
                total_diamonds = account_state.get("total_diamond", 0)
                if total_diamonds < diamond_threshold:
                    continue  # Skip offer if diamonds are below the saving threshold
            
            # Special handling for skin offers
            if offer_data["item_category"][0] == "skins":
                hero_name = offer_name.split('_')[2]  # Extract hero name from the offer ID
                owned_heroes = set(account_state["equipment"]["heroes"].keys())
                if ("h_" + hero_name) not in owned_heroes:
                    continue
                owned_skins = set(account_state["equipment"]["skins"].keys())
                available_skins = set(self.item_data["skins"]["h_" + hero_name])
                remaining_skins = available_skins - owned_skins
    
                # If all skins for the hero are owned, skip the offer
                if not remaining_skins:
                    continue
                
                # Immediately choose a random skin from remaining_skins
                selected_skin = random.choice(list(remaining_skins))
                offer_data = offer_data.copy()  # Avoid modifying the original shop_offers
                offer_data["item_id"] = [selected_skin]  # Directly assign the selected skin
    
            # Final check for currency sufficiency
            if not self.has_sufficient_currency(account_state, offer_data):
                continue
    
            # Add offer to eligible offers
            eligible_offers[offer_name] = offer_data
    
        return eligible_offers

    def has_sufficient_currency(self, account_state, offer):
        """
        Check if the account has enough currency for the offer.
        """
        cost_type = offer["cost_type"].replace("currency_", "")
        cost_amount = offer["cost_amount"]
        current_balance = account_state.get(f"total_{cost_type}", 0)
        return current_balance >= cost_amount

    def apply_purchase(self, account_state, offer, account_id, session_id, event_date, account_map_data, events, start_timestamp_fix):
        """
        Apply the purchase by deducting resources, generating events for rewards, and injecting errors.
        """
        cost_type = offer["cost_type"].replace("currency_", "")
        cost_amount = offer["cost_amount"]
    
        # Generate sink_item event
        sink_event = self.event_handler.write_event(
            event_type="resource",
            event_subtype="sink_item",
            event_date=event_date,
            account_id=account_id,
            session_id=session_id,
            item_category="currency",
            item_id=offer["cost_type"],
            item_amount=cost_amount,
            reason=offer["offer_id"]
        )
        sink_event, terminate_session = self.error_generator.attempt_event_replacement(
            sink_event, account_map_data, events, start_timestamp_fix
        )
        if terminate_session:
            return None, True

        account_state[f"total_{cost_type}"] -= cost_amount
        events.append(sink_event)
        purchase_events = []
    
        # Process rewards and generate source_item events
        for reward_category, reward_id, reward_amount in zip(
            offer["item_category"], offer["item_id"], offer["item_amount"]
        ):
            if callable(reward_id):
                reward_id = reward_id()  # Resolve callable reward IDs
    
            # Generate source_item event
            source_event = self.event_handler.write_event(
                event_type="resource",
                event_subtype="source_item",
                event_date=event_date + timedelta(seconds=1),
                account_id=account_id,
                session_id=session_id,
                item_category=reward_category,
                item_id=reward_id,
                item_amount=reward_amount,
                reason=offer["offer_id"]
            )
            source_event, terminate_session = self.error_generator.attempt_event_replacement(
                source_event, account_map_data, events, start_timestamp_fix
            )
            if terminate_session:
                return None, True
            if source_event:
                purchase_events.append(source_event)
                # Handle rewards by category
                if reward_category == "chests":
                    self.chest_handler.add_chest_to_inventory(account_state, reward_id, reward_amount)
                elif reward_category in ["heroes", "skins"]:
                    account_state["equipment"][reward_category][reward_id] = 1
                    account_state["__gear_dirty__"] = True
                elif reward_category in ["weapons", "held_items", "armor"]:
                    account_state["equipment"][reward_category][reward_id] = (
                        account_state["equipment"][reward_category].get(reward_id, 0) + reward_amount
                    )
                    account_state["__gear_dirty__"] = True
                elif reward_category == "currency":
                    # Correct handling for "currency"
                    if reward_id == "currency_gold":
                        account_state["total_gold"] += reward_amount
                    elif reward_id == "currency_diamond":
                        account_state["total_diamond"] += reward_amount
    
        return purchase_events, False

    def generate_in_game_purchase_event(self, account_id, session_id, base_timestamp, account_state, account_map_data, events, start_timestamp_fix):
        """
        Generate an in-game purchase event, apply purchase logic, and handle error injection.
        """
        eligible_offers = self.filter_offers(account_state, account_map_data)
        if not eligible_offers:
            return base_timestamp, [], False  # No offers available

        # Select a random eligible offer
        offer_name, offer_data = random.choice(list(eligible_offers.items()))

        # Final check for currency sufficiency
        cost_type = offer_data["cost_type"].replace("currency_", "")
        if account_state.get(f"total_{cost_type}", 0) < offer_data["cost_amount"]:
            return base_timestamp, [], False

        # Apply the purchase
        purchase_events, terminate_session = self.apply_purchase(
            account_state, offer_data, account_id, session_id, base_timestamp, account_map_data, events, start_timestamp_fix
        )
        if terminate_session:
            return base_timestamp, [], True

        return base_timestamp + timedelta(seconds=random.randint(10, 30)), purchase_events, False

    def combine_items(self, account_state, account_id, session_id, event_date, account_map_data, events, start_timestamp_fix):
        """
        Combine three identical items into one of a higher rarity level as long as the player has enough resources and valid items.
        """
        # === Helper: Get combination cost based on rarity upgrade ===
        def get_combination_cost(from_rarity, to_rarity):
            combination_costs = C.combination_costs
            return combination_costs.get((from_rarity, to_rarity))
    
        # === Helper: Check if account has enough currency ===
        def has_sufficient_currency(from_rarity, to_rarity):
            cost = get_combination_cost(from_rarity, to_rarity)
            if not cost:
                return False
            return (
                account_state["total_gold"] >= cost["gold"]
                and account_state["total_diamond"] >= cost["diamond"]
            )
    
        # === Helper: Apply currency sink for combination ===
        def apply_combination_cost(account_state, event_date):
            events_created = []
            if cost["gold"] > 0:
                sink_gold_event = self.event_handler.write_event(
                    event_type="resource",
                    event_subtype="sink_item",
                    event_date=event_date,
                    account_id=account_id,
                    session_id=session_id,
                    item_category="currency",
                    item_id="currency_gold",
                    item_amount=cost["gold"],
                    reason="item_combination"
                )
                sink_gold_event, terminate = self.error_generator.attempt_event_replacement(
                    sink_gold_event, account_map_data, events, start_timestamp_fix
                )
                if terminate:
                    return [], True
                events_created.append(sink_gold_event)
                account_state["total_gold"] -= cost["gold"]
    
            if cost["diamond"] > 0:
                sink_diamond_event = self.event_handler.write_event(
                    event_type="resource",
                    event_subtype="sink_item",
                    event_date=event_date,
                    account_id=account_id,
                    session_id=session_id,
                    item_category="currency",
                    item_id="currency_diamond",
                    item_amount=cost["diamond"],
                    reason="item_combination"
                )
                sink_diamond_event, terminate = self.error_generator.attempt_event_replacement(
                    sink_diamond_event, account_map_data, events, start_timestamp_fix
                )
                if terminate:
                    return [], True
                events_created.append(sink_diamond_event)
                account_state["total_diamond"] -= cost["diamond"]
    
            return events_created, False
    
        # === Main loop ===
        while True:
            # Step 1: Identify valid upgradable items (count >= 3 AND affordable)
            event_date += timedelta(seconds=random.randint(1, 3))
            valid_upgrades = []
            for category, items in account_state["equipment"].items():
                for item_id, count in items.items():
                    if count < 3:
                        continue
                    if "_" not in item_id:
                        continue
                    rarity, base_item = item_id.split("_", 1)
                    rarity_levels = list(C.item_success_contributions["rarity"].keys())
                    if rarity not in rarity_levels:
                        continue
                    idx = rarity_levels.index(rarity)
                    if idx >= len(rarity_levels) - 1:
                        continue
                    next_rarity = rarity_levels[idx + 1]
                    if not has_sufficient_currency(rarity, next_rarity):
                        continue
                    valid_upgrades.append({
                        "category": category,
                        "item_id": item_id,
                        "rarity_from": rarity,
                        "rarity_to": next_rarity,
                        "base_item": base_item
                    })
    
            # Step 2: Exit if nothing is possible
            if not valid_upgrades:
                break
    
            # Step 3: Pick random valid upgrade
            upgrade = random.choice(valid_upgrades)
            category = upgrade["category"]
            item_id = upgrade["item_id"]
            rarity_from = upgrade["rarity_from"]
            rarity_to = upgrade["rarity_to"]
            base_item = upgrade["base_item"]
            new_item_id = f"{rarity_to}_{base_item}"
    
            # Step 4: Get cost
            cost = get_combination_cost(rarity_from, rarity_to)
            if not cost:
                break  # Just in case
    
            # Step 5: Apply currency cost
            currency_events, terminate = apply_combination_cost(account_state, event_date)
            if terminate:
                return True, None
            events.extend(currency_events)
    
            # Step 6: Log sink_item for items
            event_date += timedelta(seconds=random.randint(1, 3))
            sink_event = self.event_handler.write_event(
                event_type="resource",
                event_subtype="sink_item",
                event_date=event_date,
                account_id=account_id,
                session_id=session_id,
                item_category=category,
                item_id=item_id,
                item_amount=3,
                reason="item_combination"
            )
            sink_event, terminate = self.error_generator.attempt_event_replacement(
                sink_event, account_map_data, events, start_timestamp_fix
            )
            if terminate:
                return True, None
            if sink_event:
                events.append(sink_event)
                account_state["equipment"][category][item_id] -= 3
                if account_state["equipment"][category][item_id] <= 0:
                    del account_state["equipment"][category][item_id]
                account_state["__gear_dirty__"] = True
    
            # Step 7: Log source_item for upgraded item
            event_date += timedelta(seconds=random.randint(1, 3))
            source_event = self.event_handler.write_event(
                event_type="resource",
                event_subtype="source_item",
                event_date=event_date,
                account_id=account_id,
                session_id=session_id,
                item_category=category,
                item_id=new_item_id,
                item_amount=1,
                reason="item_combination"
            )
            source_event, terminate = self.error_generator.attempt_event_replacement(
                source_event, account_map_data, events, start_timestamp_fix
            )
            if terminate:
                return True, None
            if source_event:
                events.append(source_event)
                account_state["equipment"][category][new_item_id] = (
                    account_state["equipment"][category].get(new_item_id, 0) + 1
                )
                account_state["__gear_dirty__"] = True
    
        return False, event_date  # No termination