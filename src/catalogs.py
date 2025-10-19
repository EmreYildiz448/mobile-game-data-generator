import random

event_master_dict = {
    "authentication": {
        "user_login": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
        },
        "user_logout": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "session_duration": ""
        }
    },
    "progression": {
        "level_start": {
            "level_id": "",
            "equipped_hero": "",
            "equipped_skin": "",
            "equipped_weapon": "",
            "equipped_held_item": "",
            "equipped_armor": "",
        },
        "level_success": {
            "level_id": "",
            "equipped_hero": "",
            "equipped_skin": "",
            "equipped_weapon": "",
            "equipped_held_item": "",
            "equipped_armor": "",
            "time": "",
            "total_score": "",
            "stars_gained": "",
        },
        "level_fail": {
            "level_id": "",
            "equipped_hero": "",
            "equipped_skin": "",
            "equipped_weapon": "",
            "equipped_held_item": "",
            "equipped_armor": "",
            "time": "",
            "total_score": "",
        },
        "level_abandon": {
            "level_id": "",
            "equipped_hero": "",
            "equipped_skin": "",
            "equipped_weapon": "",
            "equipped_held_item": "",
            "equipped_armor": "",
            "time": "",
            "abandon_reason": "",
        },
    },
    "resource": {
        "source_item": {
            "item_category": "",
            "item_id": "",
            "item_amount": "",
            "reason": "",
        },
        "sink_item": {
            "item_category": "",
            "item_id": "",
            "item_amount": "",
            "reason": "",
        },
    },
    "error": {
        "app_crash": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "error_id": "",
            "error_context": ""
        },
        "network_error": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "error_id": "",
            "error_context": ""
        },
        "resource_fail": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "error_id": "",
            "error_context": ""
        },
        "authentication_error": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "error_id": "",
            "error_context": ""
        },
        "transaction_error": {
            "device_model": "",
            "os_version": "",
            "app_version": "",
            "error_id": "",
            "error_context": ""
        }
    },
    "ad": {
        "ad_shown": {
            "ad_id": "",
        },
        "ad_skipped": {
            "ad_id": "",
            "watched_seconds": "",
            "remaining_seconds": ""
        },
        "ad_completed": {
            "ad_id": "",
        },
        "reward_ad_rejected": {
            "ad_id": "",
            "reward_category": "",
            "reward_id": "",
            "reward_amount": ""
        },
        "reward_ad_shown": {
            "ad_id": "",
            "reward_category": "",
            "reward_id": "",
            "reward_amount": ""
        },
        "reward_ad_skipped": {
            "ad_id": "",
            "reward_category": "",
            "reward_id": "",
            "reward_amount": "",
            "watched_seconds": "",
            "remaining_seconds": ""
        },
        "reward_ad_completed": {
            "ad_id": "",
            "reward_category": "",
            "reward_id": "",
            "reward_amount": ""
        }
    },
    "business": {
        "business": {
            "offer_id": "",
            "reward_category": [],
            "reward_id": [],
            "reward_amount": [],
            "cost_type": "",
            "cost_amount": 0.0,
            "currency_name": "",
            "exchange_rate": 1.0
        }
    }
}

level_data = {
    # Tutorial Levels
    'TUTORIAL_001': {
        'difficulty': 0.0,
        'item_amount': [1],
        'item_category': ['heroes'],
        'item_id': [lambda: random.choice(["h_warrior", "h_rogue", "h_battlemage"])],
        'success_score_floor': 500,
        'three_stars_floor': 1000,
        'time': 240,
        'two_stars_floor': 750
    },
    'TUTORIAL_002': {
        'difficulty': 0.0,
        'item_amount': [1, 1, 1],
        'item_category': ['equipment', 'equipment', 'equipment'],
        'item_id': [
            lambda: f"c_{random.choice(item_data['equipment'][random.choice(['weapons'])])}",
            lambda: f"c_{random.choice(item_data['equipment'][random.choice(['held_items'])])}",
            lambda: f"c_{random.choice(item_data['equipment'][random.choice(['armor'])])}"
        ],
        'success_score_floor': 600,
        'three_stars_floor': 1200,
        'time': 240,
        'two_stars_floor': 900
    },
    'TUTORIAL_003': {
        'difficulty': 0.0,
        'item_amount': [250, 50],
        'item_category': ['currency', 'currency'],
        'item_id': ['currency_gold', 'currency_diamond'],
        'success_score_floor': 700,
        'three_stars_floor': 1400,
        'time': 240,
        'two_stars_floor': 1050
    },
    # Regular Levels
    'LEVEL_001': {
        'difficulty': 0.1,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 800,
        'three_stars_floor': 1600,
        'time': 240,
        'two_stars_floor': 1200
    },
    'LEVEL_002': {
        'difficulty': 0.15,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 850,
        'three_stars_floor': 1700,
        'time': 240,
        'two_stars_floor': 1275
    },
    'LEVEL_003': {
        'difficulty': 0.20,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 900,
        'three_stars_floor': 1800,
        'time': 240,
        'two_stars_floor': 1350
    },
    'LEVEL_004': {
        'difficulty': 0.25,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 950,
        'three_stars_floor': 1900,
        'time': 240,
        'two_stars_floor': 1425
    },
    'LEVEL_005': {
        'difficulty': 0.30,
        'item_amount': [50],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 1000,
        'three_stars_floor': 2000,
        'time': 240,
        'two_stars_floor': 1500
    },
    'LEVEL_006': {
        'difficulty': 0.35,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1050,
        'three_stars_floor': 2100,
        'time': 240,
        'two_stars_floor': 1575
    },
    'LEVEL_007': {
        'difficulty': 0.4,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1100,
        'three_stars_floor': 2200,
        'time': 240,
        'two_stars_floor': 1650
    },
    'LEVEL_008': { # Artificially increased difficulty for analysis purposes
        'difficulty': 0.7,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1150,
        'three_stars_floor': 2300,
        'time': 240,
        'two_stars_floor': 1725
    },
    'LEVEL_009': {
        'difficulty': 0.5,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1200,
        'three_stars_floor': 2400,
        'time': 240,
        'two_stars_floor': 1800
    },
    'LEVEL_010': {
        'difficulty': 0.6,
        'item_amount': [1],
        'item_category': ['chests'],
        'item_id': ['ch_uncommon'],
        'success_score_floor': 1250,
        'three_stars_floor': 2500,
        'time': 420,
        'two_stars_floor': 1875
    },
    'LEVEL_011': {
        'difficulty': 0.65,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1300,
        'three_stars_floor': 2600,
        'time': 240,
        'two_stars_floor': 1950
    },
    'LEVEL_012': {
        'difficulty': 0.7,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1350,
        'three_stars_floor': 2700,
        'time': 240,
        'two_stars_floor': 2025
    },
    'LEVEL_013': {
        'difficulty': 0.8,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1400,
        'three_stars_floor': 2800,
        'time': 240,
        'two_stars_floor': 2100
    },
    'LEVEL_014': {
        'difficulty': 0.9,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1450,
        'three_stars_floor': 2900,
        'time': 240,
        'two_stars_floor': 2175
    },
    'LEVEL_015': {
        'difficulty': 0.95,
        'item_amount': [75],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 1500,
        'three_stars_floor': 3000,
        'time': 240,
        'two_stars_floor': 2250
    },
    'LEVEL_016': {
        'difficulty': 1.0,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1550,
        'three_stars_floor': 3100,
        'time': 240,
        'two_stars_floor': 2325
    },
    'LEVEL_017': {
        'difficulty': 1.05,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1600,
        'three_stars_floor': 3200,
        'time': 240,
        'two_stars_floor': 2400
    },
    'LEVEL_018': {
        'difficulty': 1.1,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1650,
        'three_stars_floor': 3300,
        'time': 240,
        'two_stars_floor': 2475
    },
    'LEVEL_019': {
        'difficulty': 1.15,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1700,
        'three_stars_floor': 3400,
        'time': 240,
        'two_stars_floor': 2550
    },
    'LEVEL_020': {
        'difficulty': 1.2,
        'item_amount': [1],
        'item_category': ['chests'],
        'item_id': ['ch_rare'],
        'success_score_floor': 1750,
        'three_stars_floor': 3500,
        'time': 420,
        'two_stars_floor': 2625
    },
    'LEVEL_021': {
        'difficulty': 1.25,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1800,
        'three_stars_floor': 3600,
        'time': 240,
        'two_stars_floor': 2700
    },
    'LEVEL_022': { # Artificially decreased difficulty for analysis purposes
        'difficulty': 0.7,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1850,
        'three_stars_floor': 3700,
        'time': 240,
        'two_stars_floor': 2775
    },
    'LEVEL_023': {
        'difficulty': 1.35,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1900,
        'three_stars_floor': 3800,
        'time': 240,
        'two_stars_floor': 2850
    },
    'LEVEL_024': {
        'difficulty': 1.4,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 1950,
        'three_stars_floor': 3900,
        'time': 240,
        'two_stars_floor': 2925
    },
    'LEVEL_025': {
        'difficulty': 1.45,
        'item_amount': [100],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 2000,
        'three_stars_floor': 4000,
        'time': 240,
        'two_stars_floor': 3000
    },
    'LEVEL_026': {
        'difficulty': 1.5,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2050,
        'three_stars_floor': 4100,
        'time': 240,
        'two_stars_floor': 3075
    },
    'LEVEL_027': {
        'difficulty': 1.55,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2100,
        'three_stars_floor': 4200,
        'time': 240,
        'two_stars_floor': 3150
    },
    'LEVEL_028': {
        'difficulty': 1.6,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2150,
        'three_stars_floor': 4300,
        'time': 240,
        'two_stars_floor': 3225
    },
    'LEVEL_029': {
        'difficulty': 1.65,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2200,
        'three_stars_floor': 4400,
        'time': 240,
        'two_stars_floor': 3300
    },
    'LEVEL_030': {
        'difficulty': 1.7,
        'item_amount': [1],
        'item_category': ['chests'],
        'item_id': ['ch_rare'],
        'success_score_floor': 2250,
        'three_stars_floor': 4500,
        'time': 420,
        'two_stars_floor': 3375
    },
    'LEVEL_031': {
        'difficulty': 1.75,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2300,
        'three_stars_floor': 4600,
        'time': 240,
        'two_stars_floor': 3450
    },
    'LEVEL_032': {
        'difficulty': 1.8,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2350,
        'three_stars_floor': 4700,
        'time': 240,
        'two_stars_floor': 3525
    },
    'LEVEL_033': {
        'difficulty': 1.85,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2400,
        'three_stars_floor': 4800,
        'time': 240,
        'two_stars_floor': 3600
    },
    'LEVEL_034': {
        'difficulty': 1.9,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2450,
        'three_stars_floor': 4900,
        'time': 240,
        'two_stars_floor': 3675
    },
    'LEVEL_035': {
        'difficulty': 1.95,
        'item_amount': [125],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 2500,
        'three_stars_floor': 5000,
        'time': 240,
        'two_stars_floor': 3750
    },
    'LEVEL_036': {
        'difficulty': 2.0,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2550,
        'three_stars_floor': 5100,
        'time': 240,
        'two_stars_floor': 3825
    },
    'LEVEL_037': { # Artificially increased difficulty for analysis purposes
        'difficulty': 2.7,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2600,
        'three_stars_floor': 5200,
        'time': 240,
        'two_stars_floor': 3900
    },
    'LEVEL_038': {
        'difficulty': 2.1,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2650,
        'three_stars_floor': 5300,
        'time': 240,
        'two_stars_floor': 3975
    },
    'LEVEL_039': {
        'difficulty': 2.15,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2700,
        'three_stars_floor': 5400,
        'time': 240,
        'two_stars_floor': 4050
    },
    'LEVEL_040': {
        'difficulty': 2.2,
        'item_amount': [2],
        'item_category': ['chests'],
        'item_id': ['ch_rare'],
        'success_score_floor': 2750,
        'three_stars_floor': 5500,
        'time': 420,
        'two_stars_floor': 4125
    },
    'LEVEL_041': {
        'difficulty': 2.25,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2800,
        'three_stars_floor': 5600,
        'time': 240,
        'two_stars_floor': 4200
    },
    'LEVEL_042': {
        'difficulty': 2.3,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2850,
        'three_stars_floor': 5700,
        'time': 240,
        'two_stars_floor': 4275
    },
    'LEVEL_043': {
        'difficulty': 2.35,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2900,
        'three_stars_floor': 5800,
        'time': 240,
        'two_stars_floor': 4350
    },
    'LEVEL_044': { # Artificially decreased difficulty for analysis purposes
        'difficulty': 1.7,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 2950,
        'three_stars_floor': 5900,
        'time': 240,
        'two_stars_floor': 4425
    },
    'LEVEL_045': {
        'difficulty': 2.45,
        'item_amount': [150],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 3000,
        'three_stars_floor': 6000,
        'time': 240,
        'two_stars_floor': 4500
    },
    'LEVEL_046': {
        'difficulty': 2.5,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3050,
        'three_stars_floor': 6100,
        'time': 240,
        'two_stars_floor': 4575
    },
    'LEVEL_047': {
        'difficulty': 2.55,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3100,
        'three_stars_floor': 6200,
        'time': 240,
        'two_stars_floor': 4650
    },
    'LEVEL_048': {
        'difficulty': 2.6,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3150,
        'three_stars_floor': 6300,
        'time': 240,
        'two_stars_floor': 4725
    },
    'LEVEL_049': {
        'difficulty': 2.65,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3200,
        'three_stars_floor': 6400,
        'time': 240,
        'two_stars_floor': 4800
    },
    'LEVEL_050': {
        'difficulty': 2.7,
        'item_amount': [1],
        'item_category': ['chests'],
        'item_id': ['ch_epic'],
        'success_score_floor': 3250,
        'three_stars_floor': 6500,
        'time': 420,
        'two_stars_floor': 4875
    },
    'LEVEL_051': {
        'difficulty': 2.75,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3300,
        'three_stars_floor': 6600,
        'time': 240,
        'two_stars_floor': 4950},
    'LEVEL_052': {
        'difficulty': 2.8,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3350,
        'three_stars_floor': 6700,
        'time': 240,
        'two_stars_floor': 5025},
    'LEVEL_053': {
        'difficulty': 2.85,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3400,
        'three_stars_floor': 6800,
        'time': 240,
        'two_stars_floor': 5100},
    'LEVEL_054': {
        'difficulty': 2.9,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3450,
        'three_stars_floor': 6900,
        'time': 240,
        'two_stars_floor': 5175},
    'LEVEL_055': {
        'difficulty': 2.95,
        'item_amount': [175],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 3500,
        'three_stars_floor': 7000,
        'time': 240,
        'two_stars_floor': 5250},
    'LEVEL_056': {
        'difficulty': 3.0,
        'item_amount': [300],
        'item_category': ['currency'], 
        'item_id': ['currency_gold'],
        'success_score_floor': 3550,
        'three_stars_floor': 7100,
        'time': 240,
        'two_stars_floor': 5325},
    'LEVEL_057': {
        'difficulty': 3.05,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3600,
        'three_stars_floor': 7200,
        'time': 240,
        'two_stars_floor': 5400},
    'LEVEL_058': {
        'difficulty': 3.1,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3650,
        'three_stars_floor': 7300,
        'time': 240,
        'two_stars_floor': 5475},
    'LEVEL_059': {
        'difficulty': 3.15,
        'item_amount': [300],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3700,
        'three_stars_floor': 7400,
        'time': 240,
        'two_stars_floor': 5550},
    'LEVEL_060': {
        'difficulty': 3.2,
        'item_amount': [3],
        'item_category': ['chests'],
        'item_id': ['ch_rare'],
        'success_score_floor': 3750,
        'three_stars_floor': 7500,
        'time': 420,
        'two_stars_floor': 5625},
    'LEVEL_061': {
        'difficulty': 3.25,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3800,
        'three_stars_floor': 7600,
        'time': 240,
        'two_stars_floor': 5700},
    'LEVEL_062': {
        'difficulty': 3.3,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3850,
        'three_stars_floor': 7700,
        'time': 240,
        'two_stars_floor': 5775},
    'LEVEL_063': {
        'difficulty': 3.35,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3900,
        'three_stars_floor': 7800,
        'time': 240,
        'two_stars_floor': 5850},
    'LEVEL_064': {
        'difficulty': 3.4,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 3950,
        'three_stars_floor': 7900,
        'time': 240,
        'two_stars_floor': 5925},
    'LEVEL_065': {
        'difficulty': 3.45,
        'item_amount': [200],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 4000,
        'three_stars_floor': 8000,
        'time': 240,
        'two_stars_floor': 6000},
    'LEVEL_066': {
        'difficulty': 3.5,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4050,
        'three_stars_floor': 8100,
        'time': 240,
        'two_stars_floor': 6075},
    'LEVEL_067': {
        'difficulty': 3.55,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4100,
        'three_stars_floor': 8200,
        'time': 240,
        'two_stars_floor': 6150},
    'LEVEL_068': {
        'difficulty': 3.6,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4150,
        'three_stars_floor': 8300,
        'time': 240,
        'two_stars_floor': 6225},
    'LEVEL_069': {
        'difficulty': 3.65,
        'item_amount': [400],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4200,
        'three_stars_floor': 8400,
        'time': 240,
        'two_stars_floor': 6300},
    'LEVEL_070': {
        'difficulty': 3.7,
        'item_amount': [5],
        'item_category': ['chests'],
        'item_id': ['ch_rare'],
        'success_score_floor': 4250,
        'three_stars_floor': 8500,
        'time': 420,
        'two_stars_floor': 6375},
    'LEVEL_071': {
        'difficulty': 3.75,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4300,
        'three_stars_floor': 8600,
        'time': 240,
        'two_stars_floor': 6450},
    'LEVEL_072': {
        'difficulty': 3.8,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4350,
        'three_stars_floor': 8700,
        'time': 240,
        'two_stars_floor': 6525},
    'LEVEL_073': {
        'difficulty': 3.85,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4400,
        'three_stars_floor': 8800,
        'time': 240,
        'two_stars_floor': 6600},
    'LEVEL_074': {
        'difficulty': 3.9,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4450,
        'three_stars_floor': 8900,
        'time': 240,
        'two_stars_floor': 6675},
    'LEVEL_075': {
        'difficulty': 3.95,
        'item_amount': [225],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 4500,
        'three_stars_floor': 9000,
        'time': 240,
        'two_stars_floor': 6750},
    'LEVEL_076': {
        'difficulty': 4.0,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4550,
        'three_stars_floor': 9100,
        'time': 240,
        'two_stars_floor': 6825},
    'LEVEL_077': {
        'difficulty': 4.05,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4600,
        'three_stars_floor': 9200,
        'time': 240,
        'two_stars_floor': 6900},
    'LEVEL_078': {
        'difficulty': 4.1,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4650,
        'three_stars_floor': 9300,
        'time': 240,
        'two_stars_floor': 6975},
    'LEVEL_079': {
        'difficulty': 4.15,
        'item_amount': [450],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4700,
        'three_stars_floor': 9400,
        'time': 240,
        'two_stars_floor': 7050},
    'LEVEL_080': {
        'difficulty': 4.2,
        'item_amount': [2],
        'item_category': ['chests'],
        'item_id': ['ch_epic'],
        'success_score_floor': 4750,
        'three_stars_floor': 9500,
        'time': 420,
        'two_stars_floor': 7125},
    'LEVEL_081': {
        'difficulty': 4.25,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4800,
        'three_stars_floor': 9600,
        'time': 240,
        'two_stars_floor': 7200},
    'LEVEL_082': {
        'difficulty': 4.3,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4850,
        'three_stars_floor': 9700,
        'time': 240,
        'two_stars_floor': 7275},
    'LEVEL_083': {
        'difficulty': 4.35,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4900,
        'three_stars_floor': 9800,
        'time': 240,
        'two_stars_floor': 7350},
    'LEVEL_084': {
        'difficulty': 4.4,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 4950,
        'three_stars_floor': 9900,
        'time': 240,
        'two_stars_floor': 7425},
    'LEVEL_085': {
        'difficulty': 4.45,
        'item_amount': [250],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 5000,
        'three_stars_floor': 10000,
        'time': 240,
        'two_stars_floor': 7500},
    'LEVEL_086': {
        'difficulty': 4.5,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5050,
        'three_stars_floor': 10100,
        'time': 240,
        'two_stars_floor': 7575},
    'LEVEL_087': {
        'difficulty': 4.55,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5100,
        'three_stars_floor': 10200,
        'time': 240,
        'two_stars_floor': 7650},
    'LEVEL_088': {
        'difficulty': 4.6,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5150,
        'three_stars_floor': 10300,
        'time': 240,
        'two_stars_floor': 7725},
    'LEVEL_089': {
        'difficulty': 4.65,
        'item_amount': [500],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5200,
        'three_stars_floor': 10400,
        'time': 240,
        'two_stars_floor': 7800},
    'LEVEL_090': {
        'difficulty': 4.7,
        'item_amount': [3],
        'item_category': ['chests'],
        'item_id': ['ch_epic'],
        'success_score_floor': 5250,
        'three_stars_floor': 10500,
        'time': 420,
        'two_stars_floor': 7875},
    'LEVEL_091': {
        'difficulty': 4.75,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5300,
        'three_stars_floor': 10600,
        'time': 240,
        'two_stars_floor': 7950},
    'LEVEL_092': {
        'difficulty': 4.8,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5350,
        'three_stars_floor': 10700,
        'time': 240,
        'two_stars_floor': 8025},
    'LEVEL_093': {
        'difficulty': 4.85,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5400,
        'three_stars_floor': 10800,
        'time': 240,
        'two_stars_floor': 8100},
    'LEVEL_094': {
        'difficulty': 4.9,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5450,
        'three_stars_floor': 10900,
        'time': 240,
        'two_stars_floor': 8175},
    'LEVEL_095': {
        'difficulty': 4.95,
        'item_amount': [275],
        'item_category': ['currency'],
        'item_id': ['currency_diamond'],
        'success_score_floor': 5500,
        'three_stars_floor': 11000,
        'time': 240,
        'two_stars_floor': 8250},
    'LEVEL_096': {
        'difficulty': 5.0,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5550,
        'three_stars_floor': 11100,
        'time': 240,
        'two_stars_floor': 8325},
    'LEVEL_097': {
        'difficulty': 5.05,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5600,
        'three_stars_floor': 11200,
        'time': 240,
        'two_stars_floor': 8400},
    'LEVEL_098': {
        'difficulty': 5.1,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5650,
        'three_stars_floor': 11300,
        'time': 240,
        'two_stars_floor': 8475},
    'LEVEL_099': {
        'difficulty': 5.15,
        'item_amount': [550],
        'item_category': ['currency'],
        'item_id': ['currency_gold'],
        'success_score_floor': 5700,
        'three_stars_floor': 11400,
        'time': 240,
        'two_stars_floor': 8550},
    'LEVEL_100': {
        'difficulty': 5.2,
        'item_amount': [1],
        'item_category': ['chests'],
        'item_id': ['ch_legendary'],
        'success_score_floor': 5750,
        'three_stars_floor': 11500,
        'time': 420,
        'two_stars_floor': 8625},
}

shop_offers = {
    "off_subscription_basic": {
        "offer_id": "off_subscription_basic",
        "item_category": ["currency", "currency"],
        "item_id": ["currency_gold", "currency_diamond"],
        "item_amount": [500, 100],
        "cost_type": "money",
        "cost_amount": 1.99
    },
    "off_subscription_premium": {
        "offer_id": "off_subscription_premium",
        "item_category": ["chests", "currency", "currency"],
        "item_id": ["ch_rare", "currency_gold", "currency_diamond"],
        "item_amount": [3, 1000, 250],
        "cost_type": "money",
        "cost_amount": 5.00
    },
    "off_hero_bundle_warrior": {
        "offer_id": "off_hero_bundle_warrior",
        "item_category": ["heroes", "skins"],
        "item_id": ["h_warrior", lambda: random.choice(item_data["skins"]["h_warrior"])],
        "item_amount": [1, 1],
        "cost_type": "money",
        "cost_amount": 4.99
    },
    "off_hero_bundle_rogue": {
        "offer_id": "off_hero_bundle_rogue",
        "item_category": ["heroes", "skins"],
        "item_id": ["h_rogue", lambda: random.choice(item_data["skins"]["h_rogue"])],
        "item_amount": [1, 1],
        "cost_type": "money",
        "cost_amount": 4.99
    },
    "off_hero_bundle_battlemage": {
        "offer_id": "off_hero_bundle_battlemage",
        "item_category": ["heroes", "skins"],
        "item_id": ["h_battlemage", lambda: random.choice(item_data["skins"]["h_battlemage"])],
        "item_amount": [1, 1],
        "cost_type": "money",
        "cost_amount": 4.99
    },
    "off_hero_bundle_deluxe_warrior": {
        "offer_id": "off_hero_bundle_deluxe_warrior",
        "item_category": ["heroes", "weapons", "skins"],
        "item_id": [
            "h_warrior",
            lambda: f"e_{random.choice(item_data['equipment']['weapons'])}",
            lambda: random.choice(item_data["skins"]["h_warrior"])
        ],
        "item_amount": [1, 1, 1],
        "cost_type": "money",
        "cost_amount": 9.99
    },
    "off_hero_bundle_deluxe_rogue": {
        "offer_id": "off_hero_bundle_deluxe_rogue",
        "item_category": ["heroes", "weapons", "skins"],
        "item_id": [
            "h_rogue",
            lambda: f"e_{random.choice(item_data['equipment']['weapons'])}",
            lambda: random.choice(item_data["skins"]["h_rogue"])
        ],
        "item_amount": [1, 1, 1],
        "cost_type": "money",
        "cost_amount": 9.99
    },
    "off_hero_bundle_deluxe_battlemage": {
        "offer_id": "off_hero_bundle_deluxe_battlemage",
        "item_category": ["heroes", "weapons", "skins"],
        "item_id": [
            "h_battlemage",
            lambda: f"e_{random.choice(item_data['equipment']['weapons'])}",
            lambda: random.choice(item_data["skins"]["h_battlemage"])
        ],
        "item_amount": [1, 1, 1],
        "cost_type": "money",
        "cost_amount": 9.99
    },
    "off_item_bundle_epic": {
        "offer_id": "off_item_bundle_epic",
        "item_category": ["weapons", "held_items", "armor"],
        "item_id": [
            lambda: f"e_{random.choice(item_data['equipment']['weapons'])}",
            lambda: f"e_{random.choice(item_data['equipment']['held_items'])}",
            lambda: f"e_{random.choice(item_data['equipment']['armor'])}"
        ],
        "item_amount": [1, 1, 1],
        "cost_type": "money",
        "cost_amount": 7.99
    },
    "off_gold_500": {
        "offer_id": "off_gold_500",
        "item_category": ["currency"],
        "item_id": ["currency_gold"],
        "item_amount": [500],
        "cost_type": "currency_diamond",
        "cost_amount": 50
    },
    "off_diamond_500": {
        "offer_id": "off_diamond_500",
        "item_category": ["currency"],
        "item_id": ["currency_diamond"],
        "item_amount": [500],
        "cost_type": "money",
        "cost_amount": 4.99
    },
    "off_diamond_1500": {
        "offer_id": "off_diamond_1500",
        "item_category": ["currency"],
        "item_id": ["currency_diamond"],
        "item_amount": [1500],
        "cost_type": "money",
        "cost_amount": 9.99
    },
    "off_chest_common": {
        "offer_id": "off_chest_common",
        "item_category": ["chests"],
        "item_id": ["ch_common"],
        "item_amount": [1],
        "cost_type": "currency_gold",
        "cost_amount": 250
    },
    "off_chest_uncommon": {
        "offer_id": "off_chest_uncommon",
        "item_category": ["chests"],
        "item_id": ["ch_uncommon"],
        "item_amount": [1],
        "cost_type": "currency_gold",
        "cost_amount": 500
    },
    "off_chest_rare": {
        "offer_id": "off_chest_rare",
        "item_category": ["chests"],
        "item_id": ["ch_rare"],
        "item_amount": [1],
        "cost_type": "currency_diamond",
        "cost_amount": 100
    },
    "off_chest_epic": {
        "offer_id": "off_chest_epic",
        "item_category": ["chests"],
        "item_id": ["ch_epic"],
        "item_amount": [1],
        "cost_type": "currency_diamond",
        "cost_amount": 200
    },
    "off_chest_legendary": {
        "offer_id": "off_chest_legendary",
        "item_category": ["chests"],
        "item_id": ["ch_legendary"],
        "item_amount": [1],
        "cost_type": "money",
        "cost_amount": 19.99
    },
    "off_skin_warrior": {
        "offer_id": "off_skin_warrior",
        "item_category": ["skins"],
        "item_id": lambda: random.choice(item_data["skins"]["h_warrior"]),
        "item_amount": [1],
        "cost_type": "currency_diamond",
        "cost_amount": 300
    },
    "off_skin_rogue": {
        "offer_id": "off_skin_rogue",
        "item_category": ["skins"],
        "item_id": lambda: random.choice(item_data["skins"]["h_rogue"]),
        "item_amount": [1],
        "cost_type": "currency_diamond",
        "cost_amount": 300
    },
    "off_skin_battlemage": {
        "offer_id": "off_skin_battlemage",
        "item_category": ["skins"],
        "item_id": lambda: random.choice(item_data["skins"]["h_battlemage"]),
        "item_amount": [1],
        "cost_type": "currency_diamond",
        "cost_amount": 300
    }
}

item_data = {
    "skins": {
        "h_warrior": ["s_w_bulwark", "s_w_berserker", "s_w_chevalier"],
        "h_rogue": ["s_r_swashbuckler", "s_r_assassin", "s_r_duelist"],
        "h_battlemage": ["s_bm_avatar", "s_bm_mystic", "s_bm_warcaster"]
    },
    "chests": {
        "ch_common": lambda: f"c_{random.choice(item_data['equipment'][random.choice(['weapons', 'held_items', 'armor'])])}",
        "ch_uncommon": lambda: f"u_{random.choice(item_data['equipment'][random.choice(['weapons', 'held_items', 'armor'])])}",
        "ch_rare": lambda: f"r_{random.choice(item_data['equipment'][random.choice(['weapons', 'held_items', 'armor'])])}",
        "ch_epic": lambda: f"e_{random.choice(item_data['equipment'][random.choice(['weapons', 'held_items', 'armor'])])}",
        "ch_legendary": lambda: f"l_{random.choice(item_data['equipment'][random.choice(['weapons', 'held_items', 'armor'])])}"
    },
    "equipment": {
        "weapons": ["wp_longsword", "wp_mace", "wp_dagger", "wp_crossbow", "wp_staff"],
        "held_items": ["hi_buckler", "hi_tome", "hi_potion", "hi_amulet", "hi_bomb"],
        "armor": ["arm_gambeson", "arm_barding", "arm_cloak", "arm_rune", "arm_splint"]
    },
    "currency": {
        "gold": ["currency_gold"],
        "diamond": ["currency_diamond"]
    }
}

item_success_contributions = {
    "equipment": {
        "weapons": {
            "wp_longsword": 0.05,
            "wp_mace": 0.045,
            "wp_dagger": 0.04,
            "wp_crossbow": 0.055,
            "wp_staff": 0.07  # Balanced highest contribution
        },
        "held_items": {
            "hi_buckler": 0.045,
            "hi_tome": 0.05,
            "hi_potion": 0.025,  # Balanced lowest contribution
            "hi_amulet": 0.055,
            "hi_bomb": 0.06
        },
        "armors": {
            "arm_gambeson": 0.05,
            "arm_barding": 0.045,
            "arm_cloak": 0.035, # Balanced lowest contribution
            "arm_rune": 0.07, # Balanced highest contribution
            "arm_splint": 0.055
        },
        "skins": {
            "s_w_bulwark": 0.05, 
            "s_w_berserker": 0.06,
            "s_w_chevalier": 0.025,
            "s_r_swashbuckler": 0.05,
            "s_r_assassin": 0.08,
            "s_r_duelist": 0.06,
            "s_bm_avatar": 0.05,
            "s_bm_mystic": 0.07,
            "s_bm_warcaster": 0.04
        }
    },
    "rarity": {
        "c": 1.0,    # Common
        "u": 1.2,    # Uncommon
        "r": 1.5,    # Rare
        "e": 2,      # Epic
        "l": 2.75    # Legendary
    },
    "overpowered": {
        "item_id": "wp_staff",  # Overpowered across all rarity levels
        "rarity_combination": {"item_id": "hi_buckler", "rarity": "r"}  # Rare buckler
    },
    "underpowered": {
        "item_id": "hi_potion",  # Underpowered across all rarity levels
        "rarity_combination": {"item_id": "arm_cloak", "rarity": "u"}  # Uncommon cloak
    }
}

player_archetypes = {
    # Free Users
    "free_casual": {
        "monetization_probability": 0.005,
        "retention_probability": 0.15,
        "decay_constant": 0.14,
        "session_termination_probability": 0.6,
        "ad_engagement_probability": 0.2,
        "shop_activity_probability": 0.1,
        "player_skill": (0.1, 0.3),
        "reward_ad_acceptance_probability": 0.8,
        "premium": 10, "mid_range": 40, "budget": 50,
        "full_churn": 3.5,
        "gold_upper_limit": 1000,
        "diamond_upper_limit": 50
    },
    "free_regular": {
        "monetization_probability": 0.01,
        "retention_probability": 0.25,
        "decay_constant": 0.12,
        "session_termination_probability": 0.4,
        "ad_engagement_probability": 0.4,
        "shop_activity_probability": 0.3,
        "player_skill": (0.3, 0.5),
        "reward_ad_acceptance_probability": 0.85,
        "premium": 15, "mid_range": 50, "budget": 35,
        "full_churn": 5,
        "gold_upper_limit": 750,
        "diamond_upper_limit": 50
    },
    "free_hardcore": {
        "monetization_probability": 0.025,
        "retention_probability": 0.55,
        "decay_constant": 0.1,
        "session_termination_probability": 0.2,
        "ad_engagement_probability": 0.6,
        "shop_activity_probability": 0.3,
        "player_skill": (0.5, 0.7),
        "reward_ad_acceptance_probability": 0.9,
        "premium": 20, "mid_range": 50, "budget": 30,
        "full_churn": 7.5,
        "gold_upper_limit": 500,
        "diamond_upper_limit": 300
    },

    # Low Spenders
    "low_spender_casual": {
        "monetization_probability": 0.1,
        "retention_probability": 0.25,
        "decay_constant": 0.1,
        "session_termination_probability": 0.4,
        "ad_engagement_probability": 0.3,
        "shop_activity_probability": 0.6,
        "player_skill": (0.2, 0.4),
        "reward_ad_acceptance_probability": 0.75,
        "premium": 30, "mid_range": 50, "budget": 20,
        "full_churn": 5.0,
        "gold_upper_limit": 1000,
        "diamond_upper_limit": 50
    },
    "low_spender_regular": {
        "monetization_probability": 0.25,
        "retention_probability": 0.45,
        "decay_constant": 0.08,
        "session_termination_probability": 0.3,
        "ad_engagement_probability": 0.4,
        "shop_activity_probability": 0.6,
        "player_skill": (0.4, 0.6),
        "reward_ad_acceptance_probability": 0.75,
        "premium": 40, "mid_range": 50, "budget": 10,
        "full_churn": 7,
        "gold_upper_limit": 1000,
        "diamond_upper_limit": 100
    },
    "low_spender_hardcore": {
        "monetization_probability": 0.4,
        "retention_probability": 0.65,
        "decay_constant": 0.06,
        "session_termination_probability": 0.2,
        "ad_engagement_probability": 0.5,
        "shop_activity_probability": 0.6,
        "player_skill": (0.6, 0.8),
        "reward_ad_acceptance_probability": 0.8,
        "premium": 50, "mid_range": 40, "budget": 10,
        "full_churn": 8.0,
        "gold_upper_limit": 500,
        "diamond_upper_limit": 300
    },

    # High Spenders
    "high_spender_regular": {
        "monetization_probability": 0.6,
        "retention_probability": 0.55,
        "decay_constant": 0.05,
        "session_termination_probability": 0.2,
        "ad_engagement_probability": 0.2,
        "shop_activity_probability": 0.6,
        "player_skill": (0.5, 0.7),
        "reward_ad_acceptance_probability": 0.5,
        "premium": 70, "mid_range": 25, "budget": 5,
        "full_churn": 7.5,
        "gold_upper_limit": 1000,
        "diamond_upper_limit": 300
    },
    "high_spender_hardcore": {
        "monetization_probability": 0.7,
        "retention_probability": 0.75,
        "decay_constant": 0.03,
        "session_termination_probability": 0.1,
        "ad_engagement_probability": 0.1,
        "shop_activity_probability": 0.6,
        "player_skill": (0.7, 0.9),
        "reward_ad_acceptance_probability": 0.4,
        "premium": 80, "mid_range": 15, "budget": 5,
        "full_churn": 10.0,
        "gold_upper_limit": 1000,
        "diamond_upper_limit": 300
    },
}

devices_and_os = {
    "premium": {
        "iPhone 13 Pro": {
            "base_error": 0.002,
            "os_versions": {
                "iOS 16": {"weight": 0.6, "error_multiplier": 1.0},
                "iOS 16.3.0": {"weight": 0.4, "error_multiplier": 1.25}   
            }
        },
        "Samsung Galaxy S22 Ultra": {
            "base_error": 0.001,                                          
            "os_versions": {
                "Android 13": {"weight": 1.0, "error_multiplier": 1.0}    
            }
        },
        "ASUS ROG Phone 5": {
            "base_error": 0.003,
            "os_versions": {
                "Android 12": {"weight": 1.0, "error_multiplier": 1.0}
            }
        }
    },
    "mid_range": {
        "iPhone 11": {
            "base_error": 0.005,
            "os_versions": {
                "iOS 15": {"weight": 0.6, "error_multiplier": 1.0},
                "iOS 16": {"weight": 0.3, "error_multiplier": 1.1},
                "iOS 16.3.0": {"weight": 0.1, "error_multiplier": 1.5} 
            }
        },
        "Samsung Galaxy A52": {
            "base_error": 0.004,
            "os_versions": {
                "Android 12": {"weight": 0.5, "error_multiplier": 1.0},
                "Android 13": {"weight": 0.5, "error_multiplier": 1.1}
            }
        },
        "OnePlus Nord": {
            "base_error": 0.006,
            "os_versions": {
                "Android 11": {"weight": 0.7, "error_multiplier": 1.0},
                "Android 12": {"weight": 0.3, "error_multiplier": 1.2}
            }
        }
    },
    "budget": {
        "iPhone 7": {
            "base_error": 0.012,
            "os_versions": {
                "iOS 14": {"weight": 0.5, "error_multiplier": 1.2},
                "iOS 15": {"weight": 0.5, "error_multiplier": 1.0}
            },
            "override": {
                "iOS 14": {"error_multiplier": 1.5}
            }
        },
        "Samsung Galaxy J7": {
            "base_error": 0.018,
            "os_versions": {
                "Android 10": {"weight": 1.0, "error_multiplier": 1.0}
            }
        },
        "Xiaomi Redmi Note 8": {
            "base_error": 0.010,
            "os_versions": {
                "Android 10": {"weight": 0.6, "error_multiplier": 1.0},
                "Android 11": {"weight": 0.4, "error_multiplier": 1.1}
            }
        }
    }
}

error_data = {
    "app_crash": {
        "error_id": [
            "CRASH_001", 
            "CRASH_002", 
            "CRASH_003"
        ],
        "error_context": [
            "Null pointer exception in rendering engine.",
            "Out of memory while loading assets.",
            "Segmentation fault during background task."
        ]
    },
    "network_error": {
        "error_id": [
            "NETWORK_001", 
            "NETWORK_002", 
            "NETWORK_003"
        ],
        "error_context": [
            "Timeout while fetching server data.",
            "Failed to establish a secure connection.",
            "Dropped connection due to poor signal."
        ]
    },
    "resource_fail": {
        "error_id": [
            "RESOURCE_001", 
            "RESOURCE_002", 
            "RESOURCE_003"
        ],
        "error_context": [
            "Failed to load texture assets.",
            "Corrupted data in downloaded patch.",
            "Insufficient storage for resource caching."
        ]
    },
    "authentication_error": {
        "error_id": [
            "AUTH_001", 
            "AUTH_002", 
            "AUTH_003"
        ],
        "error_context": [
            "Invalid credentials provided.",
            "Session expired during authentication.",
            "Token mismatch detected."
        ]
    },
    "transaction_error": {
        "error_id": [
            "TRANSACTION_001", 
            "TRANSACTION_002", 
            "TRANSACTION_003"
        ],
        "error_context": [
            "Payment declined by provider.",
            "Duplicate transaction detected.",
            "Failed to verify purchase receipt."
        ]
    }
}

error_map = {
    "progression": {                                        # Event type: progression
        "level_start": ["app_crash", "resource_fail"],
        "level_success": ["app_crash", "resource_fail"],
        "level_fail": ["app_crash", "resource_fail"],
        "level_abandon": ["app_crash", "resource_fail"],
    },
    "resource": {                                           # Event type: resource
        "source_item": ["app_crash", "resource_fail"],
        "sink_item": ["app_crash", "resource_fail"],
    },
    "ad": {                                                 # Event type: ad
        "ad_shown": ["network_error"],
        "ad_skipped": ["network_error"],
        "ad_completed": ["network_error"],
        "reward_ad_shown": ["network_error"],
        "reward_ad_completed": ["network_error"],
        "reward_ad_skipped": ["network_error"],
    },
    "business": {                                           # Event type: business
        "business": ["transaction_error"],
    },
    "authentication": {                                     # Event type: authentication
        "user_login": ["authentication_error"],
    },
}

ad_studio_data = {
    "Internal": {
        "ad_types": ["search", "playable", "video"],
        "engagement_effects": {"impressions": 0.95, "clicks": 1.05, "installs": 1.2, "actions": 0.95}
    },
    "Ads-R-Us": {
        "ad_types": ["video", "interstitial"],
        "engagement_effects": {"impressions": 1.4, "clicks": 1.2, "installs": 1.1, "actions": 1.0}
    },
    "Playtime": {
        "ad_types": ["playable", "interstitial"],
        "engagement_effects": {"impressions": 1.5, "clicks": 1.6, "installs": 1.4, "actions": 1.5}
    },
    "ProVis": {
        "ad_types": ["banner", "post"],
        "engagement_effects": {"impressions": 1.6, "clicks": 1.0, "installs": 0.8, "actions": 0.7}
    }
}

advertiser_config = {
    "Google Search Ads": {
        "supported_ad_types": {
            "search": ["CPC", "CPI"]
        },
        "engagement_effects": {"impressions": 1.2, "clicks": 1.4, "installs": 1.3, "actions": 1.1},
        "install_to_play_rate": 0.84
    },
    "Apple Search Ads": {
        "supported_ad_types": {
            "search": ["CPC", "CPI"]
        },
        "engagement_effects": {"impressions": 1.1, "clicks": 1.3, "installs": 1.4, "actions": 1.2},
        "install_to_play_rate": 0.82
    },
    "Google AdMob": {
        "supported_ad_types": {
            "interstitial": ["CPM", "CPI", "CPA"]
        },
        "engagement_effects": {"impressions": 1.5, "clicks": 1.8, "installs": 1.7, "actions": 1.4},
        "install_to_play_rate": 0.90
    },
    "Facebook Ads": {
        "supported_ad_types": {
            "video": ["CPM", "CPC", "CPI"],
            "post": ["CPM", "CPC"]
        },
        "engagement_effects": {"impressions": 1.1, "clicks": 1.5, "installs": 1.3, "actions": 1.2},
        "install_to_play_rate": 0.80
    },
    "TikTok Ads": {
        "supported_ad_types": {
            "video": ["CPM", "CPC", "CPI"],
            "playable": ["CPI", "CPA"],
            "post": ["CPM", "CPC"]
        },
        "engagement_effects": {"impressions": 1.4, "clicks": 0.9, "installs": 1.0, "actions": 0.8},
        "install_to_play_rate": 0.75
    },
    "Google Display Network": {
        "supported_ad_types": {
            "banner": ["CPM", "CPC"],
            "video": ["CPM", "CPI"]
        },
        "engagement_effects": {"impressions": 1.4, "clicks": 1.1, "installs": 1.1, "actions": 1.0},
        "install_to_play_rate": 0.80
    },
    "YouTube Ads": {
        "supported_ad_types": {
            "video": ["CPM", "CPC", "CPI"]
        },
        "engagement_effects": {"impressions": 1.6, "clicks": 1.4, "installs": 1.3, "actions": 1.1},
        "install_to_play_rate": 0.87
    },
    "X Ads": {
        "supported_ad_types": {
            "video": ["CPM", "CPC"],
            "post": ["CPM", "CPC"]
        },
        "engagement_effects": {"impressions": 0.75, "clicks": 0.9, "installs": 0.83, "actions": 0.75},
        "install_to_play_rate": 0.76
    },
    "Instagram Ads": {
        "supported_ad_types": {
            "video": ["CPM", "CPC", "CPI"],
            "post": ["CPM", "CPC"]
        },
        "engagement_effects": {"impressions": 1.0, "clicks": 1.4, "installs": 1.2, "actions": 1.1},
        "install_to_play_rate": 0.78
    },
    "Unity Ads": {
        "supported_ad_types": {
            "playable": ["CPI", "CPA"]
        },
        "engagement_effects": {"impressions": 1.5, "clicks": 1.1, "installs": 1.2, "actions": 1.0},
        "install_to_play_rate": 0.88
    }
}

pricing_model_effects = {
    "CPC": {"cost_range": [1.0, 2.0], "impressions": 1.2, "clicks": 1.5, "installs": 1.2, "actions": 1.1},
    "CPI": {"cost_range": [2.0, 4.0], "impressions": 1.1, "clicks": 1.15, "installs": 1.5, "actions": 1.3},
    "CPM": {"cost_range": [6.0, 8.0], "impressions": 1.5, "clicks": 1.0, "installs": 1.0, "actions": 0.8},
    "CPA": {"cost_range": [5.0, 10.0], "impressions": 1.2, "clicks": 1.3, "installs": 1.1, "actions": 1.5}
}

ad_type_effects = {
    "video": {"impressions": 1.0, "clicks": 1.5, "installs": 1.4, "actions": 1.2},
    "banner": {"impressions": 1.8, "clicks": 0.8, "installs": 0.6, "actions": 0.5},
    "interstitial": {"impressions": 1.5, "clicks": 1.2, "installs": 1.1, "actions": 1.0},
    "playable": {"impressions": 1.2, "clicks": 2.0, "installs": 1.8, "actions": 1.5},
    "search": {"impressions": 0.9, "clicks": 2.5, "installs": 1.7, "actions": 1.3},
    "post": {"impressions": 1.6, "clicks": 1.1, "installs": 0.9, "actions": 0.7}
}

ad_config_data = {
    "ad_studio_data": ad_studio_data,
    "advertiser_config": advertiser_config,
    "pricing_model_effects": pricing_model_effects,
    "ad_type_effects": ad_type_effects
}

acquisition_to_referral = {
    "YouTube Ads": "ad_yt",
    "Facebook Ads": "ad_fb",
    "Instagram Ads": "ad_ig",
    "TikTok Ads": "ad_tk",
    "X Ads": "ad_x",
    "Google Search Ads": "ad_search_engine",
    "Apple Search Ads": "ad_search_engine",
    "Unity Ads": "ad_in_app",
    "Google AdMob": "ad_in_app",
    "Google Display Network": "ad_gdn"
}

currency_profiles = {
    'United States':      {'currency_name': 'USD', 'exchange_rate': 1.00, 'market_multiplier': 1.0, 'round': 2},
    'India':              {'currency_name': 'INR', 'exchange_rate': 83.0, 'market_multiplier': 0.5, 'round': 0},
    'Brazil':             {'currency_name': 'BRL', 'exchange_rate': 5.0,  'market_multiplier': 0.6, 'round': 2},
    'United Kingdom':     {'currency_name': 'GBP', 'exchange_rate': 0.78, 'market_multiplier': 1.1, 'round': 2},
    'Canada':             {'currency_name': 'CAD', 'exchange_rate': 1.36, 'market_multiplier': 1.0, 'round': 2},
    'Australia':          {'currency_name': 'AUD', 'exchange_rate': 1.55, 'market_multiplier': 1.0, 'round': 2},
    'Germany':            {'currency_name': 'EUR', 'exchange_rate': 0.93, 'market_multiplier': 1.0, 'round': 2},
    'France':             {'currency_name': 'EUR', 'exchange_rate': 0.93, 'market_multiplier': 1.0, 'round': 2},
    'Italy':              {'currency_name': 'EUR', 'exchange_rate': 0.93, 'market_multiplier': 1.0, 'round': 2},
    'Japan':              {'currency_name': 'JPY', 'exchange_rate': 151.0,'market_multiplier': 0.7, 'round': 0},
    'South Korea':        {'currency_name': 'KRW', 'exchange_rate': 1350.0,'market_multiplier': 0.7, 'round': 0},
    'Russia':             {'currency_name': 'RUB', 'exchange_rate': 92.0, 'market_multiplier': 0.5, 'round': 0},
    'Mexico':             {'currency_name': 'MXN', 'exchange_rate': 17.0, 'market_multiplier': 0.7, 'round': 2},
    'Indonesia':          {'currency_name': 'IDR', 'exchange_rate': 16000.0, 'market_multiplier': 0.4, 'round': 0},
    'Vietnam':            {'currency_name': 'VND', 'exchange_rate': 24500.0, 'market_multiplier': 0.4, 'round': 0},
    'Turkey':             {'currency_name': 'TRY', 'exchange_rate': 32.0, 'market_multiplier': 0.6, 'round': 0},
    'Philippines':        {'currency_name': 'PHP', 'exchange_rate': 56.0, 'market_multiplier': 0.5, 'round': 0},
    'Saudi Arabia':       {'currency_name': 'SAR', 'exchange_rate': 3.75, 'market_multiplier': 0.8, 'round': 2},
    'Thailand':           {'currency_name': 'THB', 'exchange_rate': 36.0, 'market_multiplier': 0.7, 'round': 0},
    'Malaysia':           {'currency_name': 'MYR', 'exchange_rate': 4.7,  'market_multiplier': 0.6, 'round': 2},
}

regional_defaults = {
    "EU": {
        "currency_name": "EUR", "market_multiplier": 0.9, "round": 2, "countries": [
            'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic',
            'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece',
            'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg',
            'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia',
            'Slovenia', 'Spain', 'Sweden'
        ]
    },
    "MEA": {
        "currency_name": "USD", "market_multiplier": 0.5, "round": 2, "countries": [
            'Egypt', 'Morocco', 'Algeria', 'Tunisia', 'Libya', 'South Africa',
            'Nigeria', 'Kenya', 'Ghana', 'UAE', 'Kuwait', 'Oman', 'Bahrain',
            'Jordan', 'Qatar', 'Lebanon', 'Iraq'
        ]
    },
    "APAC": {
        "currency_name": "USD", "market_multiplier": 0.6, "round": 2, "countries": [
            'Indonesia', 'Vietnam', 'Thailand', 'Malaysia', 'Philippines',
            'Singapore', 'Japan', 'South Korea', 'Taiwan', 'Pakistan',
            'Bangladesh', 'Sri Lanka', 'Nepal', 'Myanmar', 'Cambodia', 'New Zealand'
        ]
    },
    "SOAM": {
        "currency_name": "USD", "market_multiplier": 0.65, "round": 2, "countries": [
            'Brazil', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Ecuador',
            'Uruguay', 'Paraguay', 'Bolivia', 'Venezuela'
        ]
    },
    "GLOBAL": {
        "currency_name": "USD", "rate": 1.00, "market_multiplier": 1.0, "round": 2}
}

referral_source = [
    "friend_referral", "organic_search", "ad_yt", 
    "ad_fb", "ad_ig", "ad_tk", "ad_x",
    "ad_in_app", "ad_search_engine", "ad_gdn", None
]

creation_methods = ["email", "google", "apple", "facebook", "tiktok", "x"]

referral_creation_mapping = {
            "friend_referral": ["email", "google", "apple"],
            "organic_search": ["email", "google", "apple"],
            "ad_yt": ["google", "email", "apple"],
            "ad_fb": ["facebook", "email", "apple", "google"],
            "ad_ig": ["facebook", "email", "apple", "google"],
            "ad_tk": ["tiktok", "email", "apple", "google"],
            "ad_x": ["x", "email", "apple", "google"],
            "ad_gdn": ["google", "email", "apple"],
            "ad_in_app": ["email", "apple", "google"],
            "ad_search_engine": ["google", "email", "apple"],
            None: ["email"]  # Edge case
}

country_weights = {
            'United States': 40, 'India': 30, 'Brazil': 20, 'United Kingdom': 15,
            'Canada': 10, 'Australia': 10, 'Germany': 10, 'Japan': 5,
            'South Korea': 5, 'Mexico': 10, 'Russia': 10, 'Indonesia': 15,
            'Vietnam': 10, 'Turkey': 10, 'Philippines': 10, 'Saudi Arabia': 5,
            'Thailand': 10, 'France': 10, 'Italy': 5, 'Malaysia': 10, 'Other': 5
}

organic_keywords = [
            "match-3 puzzle game", "best mobile games 2025", "top free games", 
            "strategy game for mobile", "addictive mobile games", "multiplayer mobile games",
            "casual mobile game", "new mobile games", "fun mobile game", 
            "puzzle game like Candy Crush"
]

search_ad_keywords = [
            "tilecrashers", "free match-3", "free puzzle"
]

name_corrections = {
            "Cote d'Ivoire": "Ivory Coast",
            "Korea": "South Korea",
            "Slovakia (Slovak Republic)": "Slovakia",
            "Libyan Arab Jamahiriya": "Libya",
            "Syrian Arab Republic": "Syria"
}

base_offer_weights = {
            "off_subscription": 2.0,
            "off_subscription_premium": 1.5,
            "off_hero_bundle_warrior": 1.5,
            "off_hero_bundle_rogue": 1.5,
            "off_hero_bundle_battlemage": 1.5,
            "off_hero_bundle_deluxe_warrior": 1.3,
            "off_hero_bundle_deluxe_rogue": 1.3,
            "off_hero_bundle_deluxe_battlemage": 1.3,
            "off_item_bundle_epic": 1.0,
            "off_diamond_500": 1.0,
            "off_diamond_1500": 0.8,
            "off_chest_legendary": 0.7
}

archetype_offer_modifiers = {
            "free_casual": {
                "off_subscription": 1.0,
                "off_subscription_premium": 0.8,
                "off_item_bundle_epic": 0.8,
                "off_diamond_500": 0.9
            },
            "free_regular": {
                "off_subscription": 1.5,
                "off_subscription_premium": 1.2,
                "off_item_bundle_epic": 1.0,
                "off_diamond_500": 1.1
            },
            "free_hardcore": {
                "off_subscription": 2.0,
                "off_subscription_premium": 1.6,
                "off_item_bundle_epic": 1.5,
                "off_diamond_500": 1.3
            },
            "low_spender_casual": {
                "off_subscription": 1.2,
                "off_subscription_premium": 1.0,
                "off_item_bundle_epic": 1.0,
                "off_diamond_500": 1.1
            },
            "low_spender_regular": {
                "off_subscription": 1.6,
                "off_subscription_premium": 1.3,
                "off_item_bundle_epic": 1.2,
                "off_diamond_500": 1.2
            },
            "low_spender_hardcore": {
                "off_subscription": 2.0,
                "off_subscription_premium": 1.4,
                "off_item_bundle_epic": 1.5,
                "off_diamond_500": 1.4
            },
            "high_spender_regular": {
                "off_subscription": 1.0,
                "off_subscription_premium": 1.8,
                "off_item_bundle_epic": 1.2,
                "off_diamond_500": 1.3,
                "off_diamond_1500": 1.5,
                "off_chest_legendary": 1.2
            },
            "high_spender_hardcore": {
                "off_subscription": 0.9,
                "off_subscription_premium": 2.0,
                "off_item_bundle_epic": 1.8,
                "off_diamond_500": 1.5,
                "off_diamond_1500": 1.8,
                "off_chest_legendary": 1.5
            }
}

combination_costs = {
            ("c", "u"): {"gold": 100, "diamond": 0},
            ("u", "r"): {"gold": 250, "diamond": 0},
            ("r", "e"): {"gold": 500, "diamond": 25},
            ("e", "l"): {"gold": 1000, "diamond": 100}
}

rarity_retention_weights = {"c": 1.0, "u": 1.2, "r": 1.5, "e": 2.0, "l": 2.75}

item_subcategory_map = {"wp": "weapons", "hi": "held_items", "arm": "armor"}