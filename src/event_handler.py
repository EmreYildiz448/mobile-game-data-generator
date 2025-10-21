class EventHandler:
    def __init__(self, event_master_dict):
        self.event_master_dict = event_master_dict

    def write_event(self, event_type, event_subtype, event_date, account_id, session_id, **kwargs):
        """
        Centralized event writer that generates metadata and constructs events.
        """
        metadata = self.create_event_metadata(event_type, event_subtype, **kwargs)

        return {
            "event_type": event_type,
            "event_subtype": event_subtype,
            "event_date": event_date,
            "account_id": account_id,
            "session_id": session_id,
            "event_metadata": metadata,
        }

    def create_event_metadata(self, event_type, event_subtype, **kwargs):
        """
        Generate metadata using the `event_master_dict` template.
        """
        metadata_template = self.event_master_dict.get(event_type, {}).get(event_subtype, {}).copy()
        if not metadata_template:
            raise ValueError(f"Metadata template for {event_type}/{event_subtype} not found.")

        for key, value in kwargs.items():
            if key in metadata_template:
                metadata_template[key] = value

        return metadata_template
    
    def make_emitter(self, **static_meta):
        """
        Return a fast callable that emits events with `static_meta`
        merged into per-call metadata. Keeps construction centralized.
        """
        # strip Nones once; copy only at call
        static_meta = {k: v for k, v in static_meta.items() if v is not None}

        def emit(*, event_type: str, event_subtype: str, event_date, **meta):
            # one shallow merge per event
            event_meta = {**static_meta, **meta}
            return self.write_event(
                event_type=event_type,
                event_subtype=event_subtype,
                event_date=event_date,
                **event_meta,
            )

        return emit