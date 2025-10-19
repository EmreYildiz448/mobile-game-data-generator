from datetime import datetime, timedelta
import random

class AdCampaignGenerator:
    def __init__(self, num_ads=50, num_campaigns=20, total_accounts=1000, seed=None, config=None):
        """Initialize ad campaign generator using the new ad_config_data structure."""
        if seed:
            random.seed(seed)

        self.config = config  # Store unified config
        self.num_ads = num_ads
        self.num_campaigns = num_campaigns
        self.total_accounts = total_accounts
        self.ad_studios = list(config["ad_studio_data"].keys())

        # Step 1: Generate Campaigns First
        self.campaigns = self.generate_campaigns()
#       self.active_advertisers = list(set(camp["acquisition_source"] for camp in self.campaigns))

        # Step 2: Generate Ads (Now assigned to Campaigns/Advertisers)
        self.ads = self.generate_ads()

        # Step 3: Distribute Installs **Evenly** Across Ads First
        self.install_counts = self.distribute_installs()

        # Step 4: Apply Install Data to Ads and Back-calculate Clicks & Impressions
        self.update_ads_with_campaign_data()
        self.assign_campaign_budgets()

        # Step 5: Generate Campaign-Advertisement Mappings
        self.mappings = self.generate_mappings()

    def generate_campaigns(self):
        """
        Ensure exactly one search ad each for Apple and Google, then generate other campaigns.
        """
        campaigns = []
        advertisers = list(self.config["advertiser_config"].keys())
        random.shuffle(advertisers)
    
        # Step 1: Force exactly one Apple Search Ad and one Google Search Ad with fixed CPI model
        search_advertisers = ["Apple Search Ads", "Google Search Ads"]
        for advertiser in search_advertisers:
            campaigns.append(self.create_campaign(len(campaigns) + 1, advertiser, "CPI"))
    
        # Step 2: Generate the rest of the campaigns with valid pricing models
        standard_advertisers = [adv for adv in advertisers if adv not in search_advertisers]
        for advertiser in standard_advertisers:
            valid_ad_types = self.config["advertiser_config"][advertiser]["supported_ad_types"]
            if not valid_ad_types:
                continue  # Skip if no valid ad types
            
            # Step 2.1: Pick a random ad type from the advertiser's supported types
            selected_ad_type = random.choice(list(valid_ad_types.keys()))
            
            # Step 2.2: Retrieve valid pricing models for this ad type
            valid_pricing_models = valid_ad_types[selected_ad_type]
            if not valid_pricing_models:
                continue  # Skip if no valid pricing models
            
            # Step 2.3: Pick a valid pricing model randomly
            campaign_type = random.choice(valid_pricing_models)
    
            # Step 2.4: Create the campaign with the correct pricing model
            campaigns.append(self.create_campaign(len(campaigns) + 1, advertiser, campaign_type))
    
        return campaigns

    def create_campaign(self, campaign_id, advertiser, campaign_type):
        """Create a campaign with randomized start & end dates."""
        start_date = datetime(2025, 1, 1) + timedelta(days=(random.randint(0, 5) * 7)) if "Search" not in advertiser else datetime(2025, 1, 1)
        end_date = start_date + timedelta(days=90)
        return {
            "campaign_id": campaign_id,
            "campaign_name": f"CAMPAIGN_{str(campaign_id).zfill(3)}",
            "start_date": start_date,
            "end_date": end_date,
            "campaign_type": campaign_type,
            "budget": 0,
            "status": "active",
            "acquisition_source": advertiser,
            "associated_ads": []
        }

    def generate_ads(self):
        """Generate advertisements, ensuring each campaign gets at least one ad first, 
        while enforcing exactly ONE search ad per search campaign."""
        ads = []
        campaign_index = 0
    
        # Step 1: Ensure exactly one search ad for each search campaign (Apple & Google)
        search_campaigns = [c for c in self.campaigns if c["acquisition_source"] in ["Apple Search Ads", "Google Search Ads"]]
        for campaign in search_campaigns:
            ad = self.create_ad(len(ads) + 1, campaign, activation_date=campaign["start_date"], force_ad_type="search")
            if ad:
                ads.append(ad)
                campaign["associated_ads"].append(ad["ad_id"])
    
        # Step 2: Generate the rest of the ads, making sure we don’t add extra search ads
        while len(ads) < self.num_ads:
            campaign = self.campaigns[campaign_index % len(self.campaigns)]
            campaign_index += 1
    
            # Skip search campaigns since they already have their ads
            if campaign["acquisition_source"] in ["Apple Search Ads", "Google Search Ads"]:
                continue
    
            ad = self.create_ad(len(ads) + 1, campaign, activation_date=campaign["start_date"])
            if ad:
                ads.append(ad)
                campaign["associated_ads"].append(ad["ad_id"])
    
        return ads

    def create_ad(self, ad_id, campaign, activation_date, force_ad_type=None):
        """Create an ad with the correct properties. Pricing model is assigned later."""
        advertiser = campaign["acquisition_source"]
        valid_ad_types = list(self.config["advertiser_config"][advertiser]["supported_ad_types"].keys())
    
        if not valid_ad_types:
            return None
    
        # If forcing a specific ad type (e.g., "search"), use it; otherwise, pick randomly
        ad_type = force_ad_type if force_ad_type in valid_ad_types else random.choice(valid_ad_types)
    
        # Filter studios that support the selected ad type
        valid_studios = [studio for studio in self.ad_studios if ad_type in self.config["ad_studio_data"][studio]["ad_types"]]
        if not valid_studios:
            return None  # No suitable studios for this ad type
    
        studio = random.choice(valid_studios)
    
        return {
            "ad_id": ad_id,
            "ad_name": f"AD_{str(ad_id).zfill(3)}",
            "studio": studio,
            "launch_date": activation_date,
            "is_active": True,
            "ad_type": ad_type,
            "acquisition_source": advertiser,
            "pricing_model": None,  # Will be assigned in Step 3
            "impression_count": None,
            "click_count": None,
            "install_count": None,
            "action_count": None,
            "cost_per_interaction": None
        }

    def distribute_installs(self):
        """First divide installs evenly across all ads, then apply modifiers."""
        total_ad_signups = int(self.total_accounts * random.uniform(0.2, 0.4))
    
        # **STEP 1**: Evenly distribute installs among all advertisements
        base_installs_per_ad = total_ad_signups // len(self.ads)
        remaining_installs = total_ad_signups % len(self.ads)
    
        # Assign base installs
        installs_per_ad = {ad["ad_id"]: base_installs_per_ad for ad in self.ads}
    
        # Distribute remaining installs randomly
        extra_ad_ids = random.sample(list(installs_per_ad.keys()), remaining_installs)
        for ad_id in extra_ad_ids:
            installs_per_ad[ad_id] += 1
    
        return installs_per_ad

    def update_ads_with_campaign_data(self):
        """Assign pricing models from campaigns to ads and apply engagement effects, 
        factoring in a **linear** ad duration multiplier for the adjusted min-max range (54-90 days)."""
        current_date = datetime(2025, 4, 1)  # Assume simulation ends here
        min_days = 54  # Minimum campaign duration
        max_days = 90  # Maximum campaign duration
    
        for campaign in self.campaigns:
            campaign_pricing_model = campaign["campaign_type"]  # Pricing model is campaign_type
    
            for ad_id in campaign["associated_ads"]:
                ad = next(ad for ad in self.ads if ad["ad_id"] == ad_id)
    
                # Assign pricing model to ad
                ad["pricing_model"] = campaign_pricing_model  
    
                # Retrieve relevant engagement effects
                advertiser = ad["acquisition_source"]
                ad_type = ad["ad_type"]
                pricing_model = ad["pricing_model"]
    
                advertiser_effect = self.config["advertiser_config"][advertiser]["engagement_effects"]
                ad_type_effect = self.config["ad_type_effects"][ad_type]
                pricing_effect = self.config["pricing_model_effects"][pricing_model]
    
                noise_factor = lambda: random.uniform(0.85, 1.15)
    
                # Compute engagement duration factor (linear scaling, adjusted for 54-90 days)
                start_date = campaign["start_date"]
                days_active = (current_date - start_date).days
    
                # Ensure `days_active` is at least `min_days`
                days_active = max(min_days, min(days_active, max_days))
    
                # Linear scaling between 1.0 (54 days) and 1.5 (90 days)
                duration_multiplier = 1.0 + ((days_active - min_days) / (max_days - min_days)) * 0.5
                duration_multiplier = min(duration_multiplier, 1.5)  # Ensure it never exceeds 1.5
    
#                print(f"Duration modifier for {ad_id}: {duration_multiplier:.4f} for a total of {days_active} days")
    
                # Apply engagement scaling
                ad["install_count"] = max(1, int(
                    self.install_counts[ad["ad_id"]] * advertiser_effect["installs"] * 
                    ad_type_effect["installs"] * pricing_effect["installs"] * 
                    duration_multiplier * noise_factor()
                ))
    
                ad["click_count"] = max(1, int(
                    ad["install_count"] * 3 * advertiser_effect["clicks"] * 
                    ad_type_effect["clicks"] * pricing_effect["clicks"] * 
                    duration_multiplier * noise_factor()
                ))
    
                ad["impression_count"] = max(1, int(
                    ad["click_count"] * 20 * advertiser_effect["impressions"] * 
                    ad_type_effect["impressions"] * pricing_effect["impressions"] * 
                    duration_multiplier * noise_factor()
                ))
    
                # Apply correct action count (only for CPA)
                ad["action_count"] = 0
                if pricing_model == "CPA":
                    ad["action_count"] = max(1, int(
                        ad["install_count"] * 0.5 * advertiser_effect["actions"] * 
                        ad_type_effect["actions"] * pricing_effect["actions"] * 
                        duration_multiplier * noise_factor()
                    ))
    
                # Assign cost per interaction based on pricing model
                ad["cost_per_interaction"] = random.choice(self.config["pricing_model_effects"][pricing_model]["cost_range"])

    def assign_campaign_budgets(self):
        """Calculate budget for each campaign based on assigned ads and cost-per-interaction values."""
        for campaign in self.campaigns:
            total_cost = 0
            
            for ad_id in campaign["associated_ads"]:
                ad = next(ad for ad in self.ads if ad["ad_id"] == ad_id)
                if ad["pricing_model"] == "CPM":
                    billable = ad["impression_count"] / 1000
                elif ad["pricing_model"] == "CPC":
                    billable = ad["click_count"]
                elif ad["pricing_model"] == "CPI":
                    billable = ad["install_count"]
                else:
                    billable = ad["action_count"]
                ad_cost = ad["cost_per_interaction"] * billable
#                print(f"Cost of ad {ad_id}: {ad_cost}")
                total_cost += ad_cost  # Example for CPM
                
#            print(f"Total cost of campaign {campaign['campaign_id']}: {total_cost}")
            # Add a buffer of 10-20% to simulate a marketing budget
            budget = total_cost * random.uniform(1.1, 1.3) if total_cost > 1000 else 1000
            campaign["budget"] = round(budget, -2)
#            print(f"Budget for campaign {campaign['campaign_id']}: {campaign['budget']}")

    def generate_mappings(self):
        """Generate mappings between campaigns and ads."""
        mappings = []
        mapping_id = 1
        for campaign in self.campaigns:
            for ad_id in campaign["associated_ads"]:
                mappings.append({
                    "mapping_id": mapping_id,
                    "ad_id": ad_id,
                    "campaign_id": campaign["campaign_id"],
                    "association_start": campaign["start_date"],
                    "association_end": campaign["end_date"]
                })
                mapping_id += 1
        return mappings