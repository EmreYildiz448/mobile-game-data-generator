import random
from datetime import timedelta
from src.settings import runtime as R

class AdEventGenerator:
    def __init__(self, ad_campaigns, event_handler, error_generator, seed=None):
        """
        Initialize the AdEventGenerator.
        """
        if not ad_campaigns:
            raise ValueError("Ad campaigns cannot be None or empty.")

        self.ad_campaigns = ad_campaigns
        self._ads_rewarded = [ad for ad in self.ad_campaigns if ad["rewarded"]]
        self._ads_standard = [ad for ad in self.ad_campaigns if not ad["rewarded"]]
        self.event_handler = event_handler
        self.error_generator = error_generator
        self.ad_probability = R.AD_SHOW_PROBABILITY  # Initial ad probability
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        

    def select_ad(self, rewarded=False):
        """
        Select an ad from the available campaigns, filtering based on whether the ad is rewarded.
        """
        pool = self._ads_rewarded if rewarded else self._ads_standard
        return random.choice(pool) if pool else None

    def update_probability(self, shown):
        """
        Update the ad probability based on whether an ad was shown.
        """
        if shown:
            self.ad_probability = R.AD_SHOW_PROBABILITY
        else:
            self.ad_probability = min(1.0, self.ad_probability + R.AD_SHOW_PROBABILITY_INCREASE)

    def create_ad_event(self, event_date, event_subtype, events, account_map_data, start_timestamp_fix, ad_data, emit, **kwargs):
        """
        Create a regular (non-rewarded) ad-related event.
        """
        if ad_data is None:
            ad_data = self.select_ad(rewarded=False)  # Only call if not passed in
    
        if not ad_data:
            return False  # No valid ad found
    
        ad_event = emit(
            event_type="ad",
            event_subtype=event_subtype,
            event_date=event_date,
            ad_id=ad_data["ad_id"],
            ad_length=ad_data.get("ad_length", R.DEFAULT_AD_LENGTH),
            **kwargs
        )
        ad_event, terminate_session = self.error_generator.attempt_event_replacement(
            ad_event, account_map_data, events, start_timestamp_fix, emit
        )
        
        if ad_event:
            events.append(ad_event)
        
        return terminate_session

    def create_reward_ad_event(self, event_date, reward_ad_probability, events, account_state, account_map_data, start_timestamp_fix, emit):
        """
        Create reward ad events using only rewarded ads.
        """
        if account_state["is_subscribed"] == "premium":
            reward_event = emit(
                event_type="resource",
                event_subtype="source_item",
                event_date=event_date + timedelta(seconds=1),  # No ad delay
                item_category="currency",
                item_id="currency_diamond",
                item_amount=R.REWARD_DIAMOND_AMOUNT,
                reason="premium_reward_ad"
            )
            reward_event, terminate_session = self.error_generator.attempt_event_replacement(
                reward_event, account_map_data, events, start_timestamp_fix, emit
            )
            if reward_event:
                events.append(reward_event)
                account_state["total_diamond"] += R.REWARD_DIAMOND_AMOUNT
            return terminate_session, 0  # No ad duration
        
        if random.random() > reward_ad_probability:
            reward_ad_rejected_event = emit(
                event_type="ad",
                event_subtype="reward_ad_rejected",
                event_date=event_date,
                ad_id="None",
                reward_category="currency",
                reward_id="currency_diamond",
                reward_amount=R.REWARD_DIAMOND_AMOUNT
            )
            reward_ad_rejected_event, terminate_session = self.error_generator.attempt_event_replacement(
                reward_ad_rejected_event, account_map_data, events, start_timestamp_fix, emit
            )
            if reward_ad_rejected_event:
                events.append(reward_ad_rejected_event)
            return terminate_session, None
    
        # Select a rewarded ad
        ad_data = self.select_ad(rewarded=True)  # Ensure only rewarded ads are chosen
        if not ad_data:
            return False, None  # No valid rewarded ad found
    
        ad_shown_event = emit(
            event_type="ad",
            event_subtype="reward_ad_shown",
            event_date=event_date,
            ad_id=ad_data["ad_id"],
            reward_category="currency",
            reward_id="currency_diamond",
            reward_amount=R.REWARD_DIAMOND_AMOUNT
        )
        ad_shown_event, terminate_session = self.error_generator.attempt_event_replacement(
            ad_shown_event, account_map_data, events, start_timestamp_fix, emit
        )
        if terminate_session:
            return True, None
        events.append(ad_shown_event)
    
        ad_length = ad_data.get("ad_length", R.DEFAULT_AD_LENGTH)
        if random.random() < R.REWARD_AD_SKIP_PROBABILITY:  # 5% chance of skipping
            # Clamp the lower bound so randint never sees a min > max
            low = min(R.MIN_AD_WATCH_LENGTH, ad_length)
            watched_seconds = random.randint(low, ad_length)
            ad_skipped_event = emit(
                event_type="ad",
                event_subtype="reward_ad_skipped",
                event_date=event_date + timedelta(seconds=watched_seconds),
                ad_id=ad_data["ad_id"],
                reward_category="currency",
                reward_id="currency_diamond",
                reward_amount=R.REWARD_DIAMOND_AMOUNT,
                watched_seconds=watched_seconds,
                remaining_seconds=ad_length - watched_seconds
            )
            ad_skipped_event, terminate_session = self.error_generator.attempt_event_replacement(
                ad_skipped_event, account_map_data, events, start_timestamp_fix, emit
            )
            if ad_skipped_event:
                events.append(ad_skipped_event)
            return terminate_session, watched_seconds
        
        else:  # Ad is completed
            ad_completed_event = emit(
                event_type="ad",
                event_subtype="reward_ad_completed",
                event_date=event_date + timedelta(seconds=ad_length),
                ad_id=ad_data["ad_id"],
                reward_category="currency",
                reward_id="currency_diamond",
                reward_amount=R.REWARD_DIAMOND_AMOUNT
            )
            ad_completed_event, terminate_session = self.error_generator.attempt_event_replacement(
                ad_completed_event, account_map_data, events, start_timestamp_fix, emit
            )
            if terminate_session:
                return True, None
            events.append(ad_completed_event)
            account_state["recent_engagement_event"] = True
    
            # Add reward to the account state
            reward_event = emit(
                event_type="resource",
                event_subtype="source_item",
                event_date=event_date + timedelta(seconds=ad_length + 1),
                item_category="currency",
                item_id="currency_diamond",
                item_amount=R.REWARD_DIAMOND_AMOUNT,
                reason="reward_ad_completed"
            )
            reward_event, terminate_session = self.error_generator.attempt_event_replacement(
                reward_event, account_map_data, events, start_timestamp_fix, emit
            )
            if reward_event:
                events.append(reward_event)
                account_state["total_diamond"] += R.REWARD_DIAMOND_AMOUNT
            return terminate_session, ad_length