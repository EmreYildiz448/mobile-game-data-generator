import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import deque
import os
import multiprocessing as mp

def chunkify(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def _worker_generate_events(chunk_accounts, worker_seed, hosted_ads, worker_id=None):
    # Import catalogs INSIDE the child process to avoid pickling lambdas
    from src.catalogs import (
        level_data as _level_data, item_data as _item_data, shop_offers as _shop_offers,
        error_data as _error_data, error_map as _error_map, event_master_dict as _event_master_dict
    )
    from src.event_handler import EventHandler
    from src.generators.errors import ErrorGenerator
    from src.generators.chest_handler import ChestHandler
    from src.generators.gameplay import EventGenerator

    # Build dependencies locally in the worker
    _eh  = EventHandler(_event_master_dict)
    _err = ErrorGenerator(error_data=_error_data, error_map=_error_map, event_handler=_eh)
    _ch  = ChestHandler(item_data=_item_data, event_handler=_eh, error_generator=_err)

    eg = EventGenerator(
        account_map_data=chunk_accounts,
        level_data=_level_data,
        item_data=_item_data,
        shop_offers=_shop_offers,
        ad_campaigns=hosted_ads,
        chest_handler=_ch,
        error_data=_error_data,
        error_map=_error_map,
        seed=worker_seed,
        worker_id=worker_id
    )
    events, summaries = eg.generate_all_events()
    return list(events), eg.get_final_account_states(), summaries


from src.settings import runtime as R
from src.catalogs import ad_config_data, advertiser_config, error_data, error_map, event_master_dict, item_data, level_data, player_archetypes, shop_offers
from src.event_handler import EventHandler
from src.generators.accounts import AccountsGenerator
from src.generators.chest_handler import ChestHandler
from src.generators.errors import ErrorGenerator
from src.generators.gameplay import EventGenerator
from src.marketing.ad_generator import AdCampaignGenerator
from src.marketing.hosted_ads import HostedAdGenerator, HostedAdInteractionGenerator
# from src.io.db_writer import insert_data
from src.io.file_writer import write_tables
from src.database.bootstrap import bootstrap_bronze
from src.database.transform_layers import transform_layer
# from src.models.orm import Account, Session, Event, HostedAdCampaign, HostedAdInteraction, Advertisement, Campaign, AdCampaignMapping
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
    # Prepare shared inputs for workers. Workers will create their own
    # EventGenerator instances to avoid pickling complex objects.
    # We will split account_map_data into N chunks and process in parallel.
    # use module-level chunkify/_worker_generate_events (picklable)

    # Determine worker count capped to available CPUs
    cpu_count = os.cpu_count() or 1
    workers = min(R.NUM_WORKERS, cpu_count) if R.NUM_WORKERS and R.NUM_WORKERS > 0 else 1
    print(f"Using {workers} worker(s) for event generation (cpu_count={cpu_count})")

    # Prepare chunks
    # account_map_data may be either a list of account dicts or a dict mapping
    # account_id -> account dict depending on the AccountsGenerator implementation.
    if isinstance(account_map_data, dict):
        account_list = list(account_map_data.values())
    else:
        # assume it's already a list-like collection of account dicts
        account_list = list(account_map_data)
    if workers == 1:
        # Single-process: keep previous behavior
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
        sessions = event_generator.generate_sessions()
    else:
        chunks = chunkify(account_list, workers)
        results = []
        with ProcessPoolExecutor(max_workers=workers) as exe:
            futures = []
            for i, chunk in enumerate(chunks):
                worker_seed = (R.SEED or 0) + i + 1
                futures.append(exe.submit(_worker_generate_events, chunk, worker_seed, hosted_ads, i + 1))
            for fut in as_completed(futures):
                results.append(fut.result())

        # Merge outputs
        merged_events = []
        merged_final_states = []
        for ev_list, final_states, _summaries in results:
            merged_events.extend(ev_list)
            merged_final_states.extend(final_states)

        # Sort by event_date once
        merged_events = sorted(merged_events, key=lambda e: e["event_date"])

        _eh  = EventHandler(event_master_dict)
        _err = ErrorGenerator(error_data=error_data, error_map=error_map, event_handler=_eh)
        _ch  = ChestHandler(item_data=item_data, event_handler=_eh, error_generator=_err)

        # Centralize A/B assignment on the merged stream (one pass, first event per account)
        central_eg = EventGenerator(
            account_map_data=account_map_data,
            level_data=level_data,
            item_data=item_data,
            shop_offers=shop_offers,
            ad_campaigns=hosted_ads,
            chest_handler=_ch,   # now a real ChestHandler object
            error_data=error_data,
            error_map=error_map,
            seed=R.SEED,
        )
        central_eg.events = deque(merged_events)

        seen_accounts = set()
        for event in central_eg.events:
            acct = event["account_id"]
            if acct in seen_accounts:
                continue
            first_event_date = event["event_date"]
            acct_map = central_eg.account_map_data.get(acct)
            if acct_map:
                central_eg.assign_ab_test_version(acct, acct_map.get("device_model"), first_event_date, acct_map)
            seen_accounts.add(acct)

        # Session-id assignment + session generation
        central_eg.assign_session_ids()
        events = list(central_eg.get_events())
        sessions = central_eg.generate_sessions()
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
#    if R.WRITE_TO_DB:
#        insert_data(Account, accounts)
#        insert_data(Session, list(sessions))
#        insert_data(Event, list(events))
#        insert_data(HostedAdCampaign, hosted_ads)
#        insert_data(HostedAdInteraction, full_interactions)
#        insert_data(Advertisement, ads)
#        insert_data(Campaign, campaigns)
#        insert_data(AdCampaignMapping, mappings)
#    else:
#        print("Skipping database inserts.")

    df_accounts = pd.DataFrame(accounts)
    df_events = pd.DataFrame(events)
    df_sessions = pd.DataFrame(sessions)
    df_hosted_ads = pd.DataFrame(hosted_ads)
    df_hosted_ad_interactions = pd.DataFrame(full_interactions)
    df_ads = pd.DataFrame(ads)
    df_campaigns = pd.DataFrame(campaigns)
    df_ad_campaign_map = pd.DataFrame(mappings)

    tables = {
        "accounts": df_accounts,
        "events": df_events,
        "sessions": df_sessions,
        "hosted_ads": df_hosted_ads,
        "hosted_ad_interactions": df_hosted_ad_interactions,
        "ads": df_ads,
        "campaigns": df_campaigns,
        "ad_campaign_map": df_ad_campaign_map,
    }

    if R.WRITE_TO_FILE:
        print("Writing data files")
        write_tables(
            tables,
            out_dir=R.DATA_INT_DIR,
            fmt=R.OUTPUT_FORMAT,
            sample_rows=R.SAMPLE_ROWS,
        )
        print(f"Files written to: {R.DATA_INT_DIR}")
    else:
        print("Skipping file write.")

    if R.WRITE_TO_DUCK:
        print("Creating DuckDB file")
        if R.WRITE_TO_FILE:
            # CSV-based path (existing behavior)
            bootstrap_bronze(
                R.DUCKDB_PATH,
                data_dir=R.DATA_INT_DIR,
                schema="bronze",
                mode="replace",
            )
        else:
            # In-memory path: skip CSVs, use DataFrames
            bootstrap_bronze(
                R.DUCKDB_PATH,
                data_dir=None,
                schema="bronze",
                mode="replace",
                tables=tables,
            )

        transform_layer(R.DUCKDB_PATH, R.SQL_SLV_DIR, "silver")
        transform_layer(R.DUCKDB_PATH, R.SQL_GLD_DIR, "gold")
        transform_layer(R.DUCKDB_PATH, R.SQL_ANLYT_DIR, "analytics")

    if R.EXEC_STAT_TESTS:
        run_ab_tests(R.DUCKDB_PATH, R.REPORT_AB_DIR)
    if R.EXEC_ML_TESTS:
        run_ml_suite(R.DUCKDB_PATH, R.REPORT_ML_DIR, None)
    print("Done")

if __name__ == "__main__":
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass
    main()