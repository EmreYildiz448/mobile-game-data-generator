from sqlalchemy import Column, String, Boolean, TIMESTAMP, Integer, JSON, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = {"schema": "raw_data"}
    account_id = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    email = Column(String, nullable=False)
    email_is_anonymized = Column(Boolean, nullable=False)
    signup_date = Column(TIMESTAMP, nullable=False)
    country = Column(String, default="Unknown")
    referral_source = Column(String)
    creation_method = Column(String)
    status = Column(String, default="active")
    acquisition_metadata = Column(JSON)

class Event(Base):
    __tablename__ = "events"
    __table_args__ = {"schema": "raw_data"}
    event_id = Column(Integer, primary_key=True)
    account_id = Column(String)
    session_id = Column(Integer)
    event_type = Column(String, nullable=False)
    event_subtype = Column(String, nullable=False)
    event_date = Column(TIMESTAMP, nullable=False)
    event_metadata = Column(JSON, name="metadata")

class Session(Base):
    __tablename__ = "sessions"
    __table_args__ = {"schema": "raw_data"}
    session_id = Column(Integer, primary_key=True)
    account_id = Column(String)
    session_start = Column(TIMESTAMP, nullable=False)
    session_end = Column(TIMESTAMP)
    duration_seconds = Column(Integer)
    region = Column(String, default="Unknown")
    platform = Column(String)
    device_model = Column(String)
    os_version = Column(String)
    app_version = Column(String)
    end_reason = Column(String)

class HostedAdCampaign(Base):
    __tablename__ = "hosted_ad_campaigns"
    __table_args__ = {"schema": "raw_data"}
    ad_id = Column(String, primary_key=True)
    ad_network = Column(String, nullable=False)
    advertised_product = Column(String)
    pricing_model = Column(String, nullable=False)
    value_per_action = Column(Float, nullable=False)
    start_date = Column(TIMESTAMP, nullable=False)
    end_date = Column(TIMESTAMP)
    ad_length = Column(Integer)
    is_active = Column(Boolean, default=True)
    rewarded = Column(Boolean, default=False)

class HostedAdInteraction(Base):
    __tablename__ = "hosted_ad_interactions"
    __table_args__ = {"schema": "raw_data"}
    interaction_id = Column(Integer, primary_key=True, autoincrement=True)
    ad_id = Column(String(50), nullable=False)
    interaction_time = Column(TIMESTAMP, nullable=False)
    interaction_type = Column(String(50), nullable=False)
    revenue = Column(Float, default=0.0)
    platform = Column(String(50), nullable=True)
    region = Column(String(50), nullable=True)
    device_model = Column(String(50), nullable=True)
    account_id = Column(String)

class Advertisement(Base):
    __tablename__ = "advertisements"
    __table_args__ = {"schema": "raw_data"}
    ad_id = Column(Integer, primary_key=True, autoincrement=True)
    ad_name = Column(String(100), nullable=False)
    launch_date = Column(TIMESTAMP, nullable=False)
    is_active = Column(Boolean, default=True)
    ad_type = Column(String(50))
    impression_count = Column(Integer, default=0)
    click_count = Column(Integer, default=0)
    install_count = Column(Integer, default=0)
    action_count = Column(Integer, default=0)
    cost_per_interaction = Column(Float, nullable=False)
    studio = Column(String(100))

class Campaign(Base):
    __tablename__ = "campaigns"
    __table_args__ = {"schema": "raw_data"}
    campaign_id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_name = Column(String(100), nullable=False, unique=True)
    start_date = Column(TIMESTAMP, nullable=False)
    end_date = Column(TIMESTAMP, nullable=True)
    campaign_type = Column(String(20), nullable=False)
    budget = Column(Integer, nullable=False)
    status = Column(String(20), default="active")
    acquisition_source = Column(String(100), nullable=False)

class AdCampaignMapping(Base):
    __tablename__ = "ad_campaign_mapping"
    __table_args__ = {"schema": "raw_data"}
    mapping_id = Column(Integer, primary_key=True, autoincrement=True)
    ad_id = Column(Integer, nullable=False)
    campaign_id = Column(Integer, nullable=False)
    association_start = Column(TIMESTAMP, nullable=False)
    association_end = Column(TIMESTAMP, nullable=True)