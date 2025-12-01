from collections import defaultdict
import pandas as pd

class AnalyticsFramework:
    def __init__(self, max_days=100):
        """
        Initialize the analytics framework with default metrics and retention tracking.

        Args:
            max_days (int): Maximum number of days to track retention. Default is 30.
        """
        # Initialize counters
        self.metrics = defaultdict(int)
        self.retention_by_day = {
            day: {"churned": 0} for day in range(max_days)
        }
        self.success_by_level = defaultdict(lambda: {"success": 0, "failure": 0})
        self.shop_activity_by_archetype = defaultdict(int)
        self.monetization_by_archetype = defaultdict(int)
    
    def log_shop_activity(self, archetype):
        self.metrics["total_shop_activities"] += 1
        self.shop_activity_by_archetype[archetype] += 1

    def log_monetization(self, archetype):
        self.metrics["total_monetizations"] += 1
        self.monetization_by_archetype[archetype] += 1

    def log_level_outcome(self, level, outcome):
        if outcome == True:
            self.success_by_level[level]["success"] += 1
            self.metrics["total_level_successes"] += 1
        elif outcome == False:
            self.success_by_level[level]["failure"] += 1
            self.metrics["total_level_failures"] += 1

    def log_retention(self, day):
        """
        Log retention for a given day.
    
        Args:
            day (int): The number of days since the player's signup date.
            retained (bool): Whether the player was retained on this day.
        """
        self.retention_by_day[day]["churned"] += 1
        self.metrics[f"retention_day_{day}_churned"] += 1

    def generate_summary(self):
        """
        Generate a summary of metrics and return DataFrames for further analysis or display.
    
        Returns:
            dict: A dictionary containing DataFrames for different metrics.
        """
        # Consolidate metrics into a DataFrame
        summary_data = {
            "Metric": [
                "Total Shop Activities",
                "Total Monetizations",
                "Total Level Successes",
                "Total Level Failures",
            ],
            "Value": [
                self.metrics["total_shop_activities"],
                self.metrics["total_monetizations"],
                self.metrics["total_level_successes"],
                self.metrics["total_level_failures"],
            ],
        }
        retention_metrics = [
            {"Day": day, "Churned": data["churned"]}
            for day, data in self.retention_by_day.items()
        ]
        retention_df = pd.DataFrame(retention_metrics)
    
        summary_df = pd.DataFrame(summary_data)
        
        # Generate detailed breakdowns for archetypes and levels
        shop_archetype_df = pd.DataFrame(
            self.shop_activity_by_archetype.items(),
            columns=["Archetype", "Shop Activities"]
        )
        monetization_archetype_df = pd.DataFrame(
            self.monetization_by_archetype.items(),
            columns=["Archetype", "Monetizations"]
        )
        level_outcome_df = pd.DataFrame(
            [
                {"Level": level, "Successes": data["success"], "Failures": data["failure"]}
                for level, data in self.success_by_level.items()
            ]
        )
    
        # Return the DataFrames
        return {
            "summary_metrics": summary_df,
            "retention_metrics": retention_df,
            "shop_activity_by_archetype": shop_archetype_df,
            "monetization_by_archetype": monetization_archetype_df,
            "level_outcomes": level_outcome_df,
        }