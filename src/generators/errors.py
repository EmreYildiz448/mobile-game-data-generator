from datetime import timedelta
import random

from src.settings import runtime as R

class ErrorGenerator:
    def __init__(self, error_data, error_map, event_handler):
        """
        Initialize the ErrorGenerator with error data, error mappings, and an EventHandler.
        """
        self.error_data = error_data
        self.error_map = error_map
        self.event_handler = event_handler

    def should_inject_error(self, error_probability):
        """
        Decide whether to replace an event with an error based on the given probability.
        """
        return random.random() < error_probability

    def generate_error_event(self, event, account_map_data, emit):
        """
        Generate an error event based on the replaced event's type and subtype.
        
        Args:
            event (dict): The original event.
            account_map_data (dict): Static metadata for the account.

        Returns:
            dict: The generated error event.
        """
        event_type = event["event_type"]
        event_subtype = event["event_subtype"]
        event_date = event["event_date"]
        account_id = event["account_id"]
        session_id = event["session_id"]

        # Determine error type and subtype
        potential_errors = self.error_map.get(event_type, {}).get(event_subtype, [])
        if not potential_errors:
            return None  # No applicable errors for this event type/subtype

        error_subtype = random.choice(potential_errors)
        error_metadata = self.error_data.get(error_subtype, {})
        error_id = random.choice(error_metadata["error_id"])
        error_context = random.choice(error_metadata["error_context"])
#        print(f"Event replaced with error: {account_id}/{event_type}/{event_subtype}/{metadata}")
        # Construct and return the error event
        return emit(
            event_type="error",
            event_subtype=error_subtype,
            event_date=event_date,
            error_id=error_id,
            error_context=error_context
        )

    def attempt_event_replacement(self, event, account_map_data, events, start_timestamp_fix, emit):
        """
        Attempt to replace an event with an error event. Handle session termination if needed.

        Args:
            event (dict): The original event.
            account_map_data (dict): Static metadata for the account.
            events (list): The list of events in the session.

        Returns:
            tuple: (updated_event, terminate_session)
                - updated_event (dict or None): The original or error event. None if the session terminates.
                - terminate_session (bool): Whether the session should terminate due to the error.
        """
#        print("Event to be replaced is as such:")
#        print(event)
#        print("****************")
#        print("Account map data within attempt_event_replacement is as such:")
#        print("****************")
#        print(account_map_data)
        app_version = account_map_data["app_version"]
        os_version = account_map_data["os_version"]
        ab_error_effect = R.AB_ERROR_MULTIPLIER if (app_version == R.AB_TEST_VERSION and os_version == R.AB_CONFLICT_OS) else 1
        error_probability = account_map_data.get("error_probability", 0) * ab_error_effect

        # Determine if an error should be injected
        if not self.should_inject_error(error_probability):
            return event, False  # Return the original event and no session termination

        # Generate the error event
        error_event = self.generate_error_event(event, account_map_data, emit)
        if not error_event:
            return event, False  # If no error was generated, return the original event

        # Check if the error type requires session termination
        if error_event["event_subtype"] in R.TERMINATING_ERROR_SUBTYPES:
            user_logout_event = emit(
                event_type="authentication",
                event_subtype="user_logout",
                event_date=error_event["event_date"] + timedelta(seconds=1),
                session_duration=((error_event["event_date"] + timedelta(seconds=1)) - start_timestamp_fix).total_seconds(),
            )
            # Append the error event
            events.append(error_event)
            events.append(user_logout_event)
            return None, True  # Signal to terminate the session
        elif error_event["event_subtype"] == "transaction_error":
            events.append(error_event)
            return None, False

        return error_event, False