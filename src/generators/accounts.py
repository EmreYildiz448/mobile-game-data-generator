import math
import random
import uuid
import numpy as np
from datetime import timedelta
from faker import Faker

from src.settings import runtime as R
from src import catalogs as C

def deterministic_uuid(seed: int, index: int) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}-{index}")

class AccountProbabilityAssigner:
    def __init__(self, archetypes):
        """
        Initialize the AccountProbabilityAssigner with player archetypes.
        """
        self.archetypes = archetypes

    def assign_archetype(self):
        """
        Randomly assign a player archetype based on weighted probabilities.
        Adjust the weights based on desired distribution.
        """
        archetype_keys = list(self.archetypes.keys())
        weights = R.ARCHETYPE_WEIGHTS
        return random.choices(archetype_keys, weights=weights, k=1)[0]

    def get_archetype_probabilities(self, archetype_key):
        """
        Retrieve the probability dictionary for a specific archetype.
        """
        return self.archetypes.get(archetype_key, {})

class AccountsGenerator:
    def __init__(self, start_date, end_date, total_accounts, archetypes, ad_install_data, seed=None):
        """
        Initialize the AccountsGenerator with a date range, total accounts, and archetypes.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.total_accounts = total_accounts
        self.probability_assigner = AccountProbabilityAssigner(archetypes)
        self.ad_install_data = ad_install_data  # Store ad_install_data for later use
        self.seed = seed
        self.faker = Faker()

        if seed is not None:
            self.faker.seed_instance(seed)

        self.remaining_installs = {ad["ad_name"]: ad["install_count"] for ad in ad_install_data}
        
        # Updated referral sources with refined categories
        self.referral_source = C.referral_source

        # Updated creation methods
        self.creation_methods = C.creation_methods

        # Probability mapping from referral_source -> creation_method
        self.referral_creation_mapping = C.referral_creation_mapping

        # Define static options for constrained columns
        self.generated_accounts = []

        # Define country weights for selection
        self.country_weights = C.country_weights
        
        # Define common search keywords for organic search queries
        self.organic_keywords = C.organic_keywords

        self.search_ad_keywords = C.search_ad_keywords
        
    def generate_search_query(self):
        """
        Generate a random search query by selecting 2-3 keywords.
        """
        num_keywords = random.randint(R.SEARCH_QUERY_KEYWORDS_MIN, R.SEARCH_QUERY_KEYWORDS_MAX)
        return " ".join(random.sample(self.organic_keywords, num_keywords))
    
    def generate_signup_date(self):
        """Generate a signup date with an industry-aligned time distribution to reflect natural player behavior."""
        
        total_days = (self.end_date - self.start_date).days
        days = np.arange(1, total_days + 1)
    
        # Use a square-root function to create a smoother increase in visibility
        growth_factor = R.GROWTH_FACTOR  # Lower = flatter, Higher = steeper
        probabilities = (days ** growth_factor)  
        probabilities /= probabilities.sum()  # Normalize to sum to 1
    
        # Select a day with weighted probability
        chosen_day = np.random.choice(days, p=probabilities)
    
        # Generate a realistic industry-aligned signup hour using sinusoidal variation
        peak_hour = R.PEAK_HOUR_UTC  # Midday peak in UTC (can be adjusted based on region)
        hour_variation = int(peak_hour + R.PEAK_HOUR_AMPLITUDE * math.sin(2 * math.pi * (random.random())))  # Simulate natural player peaks
        random_minute = np.random.randint(0, 60)  # Random minute from 0 to 59
        random_second = np.random.randint(0, 60)  # Random second from 0 to 59
        
        return self.start_date + timedelta(days=int(chosen_day), hours=hour_variation, minutes=random_minute, seconds=random_second)

    def generate_device_and_os(self, archetype_key):
        """
        Generate a device, its OS version, and the final error probability based on the archetype.
        """
        archetype_data = self.probability_assigner.get_archetype_probabilities(archetype_key)
    
        # Determine the device tier based on weights in archetype data
        tier_weights = {
            "premium": archetype_data.get("premium", 0),
            "mid_range": archetype_data.get("mid_range", 0),
            "budget": archetype_data.get("budget", 0),
        }
        device_tier = random.choices(
            list(tier_weights.keys()),
            weights=list(tier_weights.values()),
            k=1
        )[0]
    
        # Select a device model from the chosen tier
        devices = list(C.devices_and_os[device_tier].keys())
        device_model = random.choice(devices)
    
        # Select an OS version for the chosen device using weights
        os_data = C.devices_and_os[device_tier][device_model]["os_versions"]
        os_versions = list(os_data.keys())
        os_weights = [os_data[os_version]["weight"] for os_version in os_versions]
        selected_os = random.choices(os_versions, weights=os_weights, k=1)[0]
    
        # Calculate the final error probability
        base_error = C.devices_and_os[device_tier][device_model]["base_error"]
        error_multiplier = os_data[selected_os]["error_multiplier"]
    
        # Check for any overrides
        override = C.devices_and_os[device_tier][device_model].get("override", {}).get(selected_os)
        if override:
            error_multiplier = override["error_multiplier"]
    
        final_error_probability = base_error * error_multiplier
    
        return device_model, selected_os, final_error_probability
        
    def generate_country(self):
        """
        Generate a country based on weighted probabilities, excluding China for 'Other' fallback.
        Applies manual corrections for inconsistent country names.
        """
        # Country name corrections for Faker inconsistencies
        name_corrections = C.name_corrections
    
        countries = list(self.country_weights.keys())
        weights = list(self.country_weights.values())
        selected_country = random.choices(countries, weights=weights, k=1)[0]
    
        # If randomly selected country is "Other", use faker
        if selected_country == "Other":
            while True:
                faker_country = self.faker.country()
                if faker_country == "China":
                    continue  # Reject China explicitly
                # Apply correction if needed
                corrected_country = name_corrections.get(faker_country, faker_country)
                return corrected_country
        else:
            return selected_country

    def get_currency_info(self, country):
        """
        Get currency_name, raw exchange_rate (vs USD), market_multiplier, and rounding_rule based on country.
        """
        if country in C.currency_profiles:
            profile = C.currency_profiles[country]
            return (
                profile['currency_name'],
                profile['exchange_rate'],          # Raw rate
                profile['market_multiplier'],      # For price adjustment
                profile['round']                   # Currency rounding
            )
    
        for region in C.regional_defaults.values():
            if "countries" in region and country in region["countries"]:
                return (
                    region["currency_name"],
                    1.0,                            # Fallback rate
                    region["market_multiplier"],
                    region["round"]
                )
    
        # Fallback to GLOBAL
        global_fallback = C.regional_defaults["GLOBAL"]
        return (
            global_fallback["currency_name"],
            1.0,                                   # Fallback rate
            global_fallback["market_multiplier"],
            global_fallback["round"]
        )
    
    def assign_referral_source(self, accounts, account_map_data):
        """
        Assign referral sources after generating accounts and devices.
        """
        # Step 1: Spend Apple Search Ads on iOS Devices
        for ad_data in self.ad_install_data:
            if ad_data["acquisition_source"] == "Apple Search Ads":
                installs = ad_data["install_count"]
                for account in accounts:
                    if installs <= 0:
                        break
                    if "iOS" in account.get("os_version", "") and account.get("referral_source") is None:
                        account["referral_source"] = "ad_search_engine"
                        account["acquisition_metadata"] = {
                            "ad_name": ad_data["ad_name"],
                            "campaign_name": ad_data["campaign_name"],
                            "search_ad_keyword": f"{random.choice(self.search_ad_keywords)} game"
                        }
                        installs -= 1
                        self.remaining_installs[ad_data["ad_name"]] -= 1
    
        # Step 2: Spend Google Search Ads on Android Devices
        for ad_data in self.ad_install_data:
            if ad_data["acquisition_source"] == "Google Search Ads":
                installs = ad_data["install_count"]
                for account in accounts:
                    if installs <= 0:
                        break
                    if "Android" in account.get("os_version", "") and account.get("referral_source") is None:
                        account["referral_source"] = "ad_search_engine"
                        account["acquisition_metadata"] = {
                            "ad_name": ad_data["ad_name"],
                            "campaign_name": ad_data["campaign_name"],
                            "search_ad_keyword": f"{random.choice(self.search_ad_keywords)} game"
                        }
                        installs -= 1
                        self.remaining_installs[ad_data["ad_name"]] -= 1
    
        # Step 3: Allocate remaining ad installs for other networks
        for ad_data in self.ad_install_data:
            ad_name = ad_data["ad_name"]
            acquisition_source = ad_data["acquisition_source"]
            installs = ad_data["install_count"]
    
            # Skip Search Ads to prevent double allocation
            if acquisition_source in ["Apple Search Ads", "Google Search Ads"]:
                continue
    
            # Determine the corresponding referral source
            referral_source = C.acquisition_to_referral.get(acquisition_source)
    
            # Allocate installs to remaining accounts
            for account in accounts:
                if installs <= 0:
                    break
                if account.get("referral_source") is None:
                    account["referral_source"] = referral_source
                    account["acquisition_metadata"] = {
                        "ad_name": ad_data["ad_name"],
                        "campaign_name": ad_data["campaign_name"]
                    }
                    installs -= 1
                    self.remaining_installs[ad_name] -= 1
    
        # Step 4: Check if all ad installs are exhausted
        all_installs_exhausted = all(count <= 0 for count in self.remaining_installs.values())
        if all_installs_exhausted:
            # Update referral source list to exclude all ad_* options
            self.referral_source = [
                "friend_referral", 
                "organic_search", 
                None  # No referral source
            ]
            friend_weight = random.uniform(R.FRIEND_WEIGHT_MIN, R.FRIEND_WEIGHT_MAX)  # Fixed weight for friend_referral
    
            # Dynamic Weighting for Organic Search
            campaign_start = R.CAMPAIGN_START
            campaign_end = R.CAMPAIGN_END
            max_organic_weight = R.MAX_ORGANIC_WEIGHT  # Maximum weight for organic search
    
            for account in accounts:
                if account.get("referral_source") is None:
                    # Calculate the time-based weight for organic_search
                    signup_date = account.get("signup_date")
                    time_passed = (signup_date - campaign_start).days
                    campaign_duration = (campaign_end - campaign_start).days
                    relative_time = time_passed / campaign_duration
    
                    # Linear increase in organic search weight
                    organic_weight = max_organic_weight * relative_time
                    none_weight = 1 - organic_weight - friend_weight
    
                    # Ensure weights add up to 1
                    weight_sum = organic_weight + friend_weight + none_weight
                    organic_weight /= weight_sum
                    friend_weight /= weight_sum
                    none_weight /= weight_sum
    
                    # Dynamic weighting for referral source selection
                    referral_source = random.choices(
                        ["friend_referral", "organic_search", None],
                        weights=[friend_weight, organic_weight, none_weight]
                    )[0]
    
                    # Assign acquisition metadata for organic_search
                    if referral_source == "organic_search":
                        acquisition_metadata = {"search_query": self.generate_search_query()}
                    
                    # Assign acquisition metadata for friend_referral
                    elif referral_source == "friend_referral":
                        if len(self.generated_accounts) > 1:
                            referring_account = random.choice(self.generated_accounts)
                            # Prevent self-referral by checking IDs
                            while referring_account["account_id"] == account["account_id"]:
                                referring_account = random.choice(self.generated_accounts)
                            
                            # Generate referral code
                            referral_code = f"{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:4]}"
                            
                            acquisition_metadata = {
                                "referral_account_id": str(referring_account["account_id"]),
                                "referral_code": referral_code  # New key-value pair
                            }
                        else:
                            acquisition_metadata = None  # In case no other accounts are present

                    else:
                        acquisition_metadata = None
    
                    account["referral_source"] = referral_source
                    account["acquisition_metadata"] = acquisition_metadata
        # Step 5: Assign creation_method based on referral_source and ad_id conditions
        for account in accounts:
            referral_source = account.get("referral_source")
            acquisition_metadata = account["acquisition_metadata"] if account["acquisition_metadata"] else {}
        
            # Check for specific ad_id cases
            ad_name = acquisition_metadata.get("ad_name")
            if ad_name == "AD_001":
                # Apple Search Ads → Exclude "google"
                creation_options = [method for method in self.referral_creation_mapping[referral_source] if method != "google"]
            elif ad_name == "AD_002":
                # Google Search Ads → Exclude "apple"
                creation_options = [method for method in self.referral_creation_mapping[referral_source] if method != "apple"]
            else:
                # General case → Use mapping as-is
                creation_options = self.referral_creation_mapping[referral_source]
        
            # Randomly assign creation_method
            account["creation_method"] = random.choice(creation_options) if creation_options else "email"

    def generate_account(self):
        """
        Generate a single account with all required fields.
        """
        signup_date = self.generate_signup_date()
        email_is_anonymized = random.choice([True, False])
        email = (
            f"user{random.randint(1000, 9999)}@example.com" if email_is_anonymized else self.faker.email()
        )
        
        account_index = len(self.generated_accounts)
        account_id = deterministic_uuid(self.seed, account_index)

        # Step 1: Generate Basic Account Data
        account_data = {
            'account_id': account_id,
            'username': self.faker.user_name(),
            'email': email,
            'email_is_anonymized': email_is_anonymized,
            'signup_date': signup_date,
            'country': self.generate_country(),
            'referral_source': None,  # Leave as None for assign_referral_source to handle
            'creation_method': None,  # To be set in assign_referral_source
            'status': 'active',
            'acquisition_metadata': None  # To be set in assign_referral_source
        }
    
        # Append to generated_accounts for future friend_referrals
        self.generated_accounts.append(account_data)
        
        return account_data

    def generate_accounts(self):
        """
        Generate all accounts and return them as a list of dictionaries, 
        along with a subset of critical data in account_map_data that includes probabilities.
        """
        accounts = [self.generate_account() for _ in range(self.total_accounts)]
        
        account_map_data = []
        for account in accounts:
            archetype_key = self.probability_assigner.assign_archetype()
            archetype_probabilities = self.probability_assigner.get_archetype_probabilities(archetype_key)
            device_model, os_version, final_error_probability = self.generate_device_and_os(archetype_key)
    
            account['device_model'] = device_model
            account['os_version'] = os_version
            currency_name, exchange_rate, market_multiplier, currency_rounding = self.get_currency_info(account['country'])
            
            account_map_data.append({
                'account_id': account['account_id'],
                'signup_date': account['signup_date'],
                'country': account['country'],
                'creation_method': account['creation_method'],
                'device_model': device_model,
                'os_version': os_version,
                'app_version': R.DEFAULT_APP_VERSION,
                'error_probability': final_error_probability,
                'archetype': archetype_key,  # Add the archetype name here
                'referred_friend': False,
                'currency_name': currency_name,
                'exchange_rate': exchange_rate,
                'market_multiplier': market_multiplier,
                'currency_rounding': currency_rounding,
                'adjusted_exchange_rate': exchange_rate * market_multiplier,
                **archetype_probabilities
            })
        
        self.assign_referral_source(accounts, account_map_data)

        referring_accounts = {}
        for account in accounts:
            acquisition_metadata = account.get("acquisition_metadata", {})
            if acquisition_metadata and "referral_account_id" in acquisition_metadata:
                referring_account = acquisition_metadata["referral_account_id"]
                referral_code = acquisition_metadata["referral_code"]
                referral_timestamp = account["signup_date"]
                referring_accounts[referring_account] = [referral_timestamp, referral_code]
        
        for account in account_map_data:
            acc_id = account["account_id"]
            if acc_id in referring_accounts.keys():
                account["referred_friend"] = True
                account["referral_timestamp"] = referring_accounts[acc_id][0]
                account["referral_code"] = referring_accounts[acc_id][1]

        accounts = sorted(accounts, key=lambda e: e["signup_date"])
        return accounts, account_map_data