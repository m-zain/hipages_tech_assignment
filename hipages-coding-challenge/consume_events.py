from event_processor import parse_event
import pandas as pd
import json
import os


processed_events = list()
batch_size = 100
current_batch_size = 0


def process_event(event):
    """
    Description: Function to process the event and parse and transform required information
    and to call export function to export parsed events in csv.
    :parameter: event as a Json.
    :return: None.
    """

    global current_batch_size
    global batch_size

    current_batch_size += 1
    # Calling custom parse_event function to process event and perform transformation on the event.
    processed_events.append(parse_event(event))

    if current_batch_size == batch_size:
        # export/append the events into csv if reached the batch size.
        dump_events_in_csv(processed_events)
        processed_events.clear()
        current_batch_size = 0


def dump_events_in_csv(events_processed):
    """
    Description: Function to export/append parsed events into csv.
    :parameter: list of events processed as a batch.
    :return: None.
    """

    processed_events_df = pd.DataFrame(events_processed)

    # if file does not exist then create file with header.
    if not os.path.isfile('user_web_stats.csv'):
        processed_events_df.to_csv("user_web_stats.csv", index=False, header=True, mode="w")
    else:
        # else if file exists so append events without the header.
        processed_events_df.to_csv("user_web_stats.csv", index=False, header=False, mode="a")


if __name__ == '__main__':
    for event in open('source_event_data.json', 'r'):
        # Event by event consumption of all the events
        process_event(json.loads(event))
