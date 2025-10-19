import pandas as pd
from datetime import datetime

from src.settings import runtime as R
from src.catalogs import ad_config_data, advertiser_config, error_data, error_map, event_master_dict, item_data, level_data, player_archetypes, shop_offers
from src.event_handler import EventHandler
from src.generators.accounts import AccountsGenerator
from src.generators.chest_handler import ChestHandler
from src.generators.errors import ErrorGenerator
from src.generators.gameplay import EventGenerator
from src.marketing.ad_generator import AdCampaignGenerator
from src.marketing.hosted_ads import HostedAdGenerator, HostedAdInteractionGenerator
from src.io.db_writer import insert_data
from src.io.file_writer import write_tables
from src.database.bootstrap import bootstrap_bronze
from src.database.transform_layers import transform_layer
from src.models.orm import Account, Session, Event, HostedAdCampaign, HostedAdInteraction, Advertisement, Campaign, AdCampaignMapping
from src.analysis.ab_test import run_ab_tests
from src.analysis.ml_models import run_ml_suite

def main():
    print(datetime.now())
    ad_campaign_generator = AdCampaignGenerator(
        num_ads=R.NUM_ADS,
        num_campaigns=R.NUM_CAMPAIGNS,
        total_accounts=R.NUM_ACCOUNTS,
        seed=R.SEED,
        config=ad_config_data
    )
    ads = ad_campaign_generator.ads
    campaigns = ad_campaign_generator.campaigns
    mappings = ad_campaign_generator.mappings
    print(f"Marketing data completed: {len(ads)} ads and {len(campaigns)} campaigns created, with a total of {len(mappings)} ad-to-campaign associations")
    ad_install_data = [
        {
            "ad_id": ad["ad_id"],
            "ad_name": ad["ad_name"],
            "campaign_id": campaign["campaign_id"],
            "campaign_name": campaign["campaign_name"],
            "launch_date": ad["launch_date"],
            "acquisition_source": ad["acquisition_source"],
            "install_count": round(ad["install_count"] * advertiser_config[campaign["acquisition_source"]]["install_to_play_rate"], 0)
        }
        for ad in ads
        for campaign in campaigns if ad["ad_id"] in campaign["associated_ads"]
    ]
    accounts_generator = AccountsGenerator(
        start_date=R.START_DATE,
        end_date=R.END_DATE,
        total_accounts=R.NUM_ACCOUNTS,
        archetypes=player_archetypes,
        ad_install_data=ad_install_data,
        seed=R.SEED
    )
    accounts, account_map_data = accounts_generator.generate_accounts()
    print(f"All accounts generated for a total of {len(accounts)} account rows")
    print(accounts[0])
    ad_generator = HostedAdGenerator(seed=R.SEED)
    hosted_ads = ad_generator.generate_all()
    event_handler = EventHandler(event_master_dict)
    error_generator = ErrorGenerator(error_data=error_data, error_map=error_map, event_handler=event_handler)
    chest_handler = ChestHandler(item_data=item_data, event_handler=event_handler, error_generator=error_generator)
    event_generator = EventGenerator(
        account_map_data=account_map_data,
        level_data=level_data,
        item_data=item_data,
        shop_offers=shop_offers,
        ad_campaigns=hosted_ads,
        chest_handler=chest_handler,
        error_data=error_data,
        error_map=error_map,
        seed=R.SEED,
    )
    events, _ = event_generator.generate_all_events() # _ -> summaries
    print(f"All events generated for a total of {len(events)} event rows")
#    final_account_states = event_generator.get_final_account_states()
    sessions = event_generator.generate_sessions()
    print(f"All sessions generated for a total of {len(sessions)} session rows")
    ad_interaction_generator = HostedAdInteractionGenerator(
        hosted_ads, 
        events,
        player_archetypes,
        account_map_data
    )
    current_time = datetime.now().strftime("%H:%M")
    print(f"Hosted ad interactions generation starts at {current_time}")
    full_interactions = ad_interaction_generator.generate_all_interactions()
    current_time = datetime.now().strftime("%H:%M")
    print(f"Hosted ad interactions generation ends at {current_time}")
    if R.WRITE_TO_DB:
        insert_data(Account, accounts)
        insert_data(Session, list(sessions))
        insert_data(Event, list(events))
        insert_data(HostedAdCampaign, hosted_ads)
        insert_data(HostedAdInteraction, full_interactions)
        insert_data(Advertisement, ads)
        insert_data(Campaign, campaigns)
        insert_data(AdCampaignMapping, mappings)
    else:
        print("Skipping database inserts.")
    if R.WRITE_TO_FILE:
        print("Writing data files")
        df_accounts = pd.DataFrame(accounts)
        df_events = pd.DataFrame(events)
        df_sessions = pd.DataFrame(sessions)
        df_hosted_ads = pd.DataFrame(hosted_ads)
        df_hosted_ad_interaction = pd.DataFrame(full_interactions)
        df_ads = pd.DataFrame(ads)
        df_campaigns = pd.DataFrame(campaigns)
        df_ad_campaign_map = pd.DataFrame(mappings)
        write_tables(
            {"accounts": df_accounts, "events": df_events, "sessions": df_sessions,
             "hosted_ads": df_hosted_ads, "hosted_ad_interactions": df_hosted_ad_interaction,
             "ads": df_ads, "campaigns": df_campaigns, "ad_campaign_map": df_ad_campaign_map},
            out_dir=R.DATA_INT_DIR,
            fmt=R.OUTPUT_FORMAT,
            sample_rows=R.SAMPLE_ROWS,
        )
        print(f"Files written to: {R.DATA_INT_DIR}")
        print(df_accounts.head())
        print(df_events.head())
        print(df_sessions.head())
        print(df_hosted_ads.head())
        print(df_hosted_ad_interaction.head())
        print(df_ads.head())
        print(df_campaigns.head())
        print(df_ad_campaign_map.head())
    else:
        print("Skipping file write.")
    if R.WRITE_TO_DUCK:
        print("Creating DuckDB file")
        bootstrap_bronze(R.DUCKDB_PATH, data_dir=R.DATA_INT_DIR, schema="bronze", mode="replace")
        transform_layer(R.DUCKDB_PATH, R.SQL_SLV_DIR, "silver")
        transform_layer(R.DUCKDB_PATH, R.SQL_GLD_DIR, "gold")
        transform_layer(R.DUCKDB_PATH, R.SQL_ANLYT_DIR, "analytics")
    if R.EXEC_STAT_TESTS:
        run_ab_tests(R.DUCKDB_PATH, R.REPORT_AB_DIR)
    if R.EXEC_ML_TESTS:
        run_ml_suite(R.DUCKDB_PATH, R.REPORT_ML_DIR, None)

if __name__ == "__main__":
    main()