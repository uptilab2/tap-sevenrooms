# TODO: What is report_type, is it unique to TikTok or something used by Reeport
STREAMS = {
    "service_type": {
        "RESERVATION": {
            "report_type": {
                "BASIC": {
                    "data_level": {
                        "AD": {
                            "unsupported_metrics": ["profile_visites", "cost_per_total_ratings", "profile_visites_rate", "comments", "advertiser_id",
                                                    "date", "shares", "aeo_type", "app_event_cost_per_add_to_cart", "likes"],
                        },
                        "ADGROUP": {
                            "unsupported_metrics": ["ad_text", "adgroup_id", "cost_per_total_ratings", "profile_visites_rate", "shares", "advertiser_id",
                                                    "ad_name", "comments", "date", "profile_visites", "app_event_cost_per_add_to_cart", "likes"],
                        },
                        "ADVERTISER": {
                            "unsupported_metrics": ["profile_visites", "ad_text", "adgroup_id", "cost_per_total_ratings", "profile_visites_rate",
                                                    "campaign_id", "shares", "adgroup_name", "ad_name", "comments", "date", "aeo_type", "campaign_name",
                                                    "app_event_cost_per_add_to_cart", "likes"],
                        },
                        "CAMPAIGN": {
                            "unsupported_metrics": ["ad_text", "adgroup_id", "cost_per_total_ratings", "profile_visites_rate", "shares", "advertiser_id",
                                                    "adgroup_name", "ad_name", "comments", "date", "aeo_type", "profile_visites",
                                                    "app_event_cost_per_add_to_cart", "likes"],
                        }
                    },
                },
                "AUDIENCE": {
                    "data_level": {
                        "AD": {
                            "unsupported_metrics": []
                        },
                        "ADGROUP": {
                            "unsupported_metrics": ["ad_name", "ad_text"]
                        },
                        "ADVERTISER": {
                            "unsupported_metrics": ["adgroup_id", "ad_name", "ad_text", "campaign_id", "adgroup_name", "campaign_name"]
                        },
                        "CAMPAIGN": {
                            "unsupported_metrics": ["adgroup_id", "ad_name", "ad_text", "adgroup_name"]
                        }
                    },
                    "dimensions": ["gender", "age", "country_code", "ac", "language", "platform", "interest_category", "placement"]
                },
                "PLAYABLE_MATERIAL": {
                    "dimensions": ["playable_id", "playable_id,country_code"]
                },
            }
        }
    }
}
