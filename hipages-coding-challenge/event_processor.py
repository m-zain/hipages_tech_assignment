import re
import urllib.parse


def urlparse(url):
    """"
    Description: Function to check if TCP exist for the url otherwise add 'TCP' and parse the url
    :parameter: URL
    :return: Parsed URL
    """

    if not re.search(r'^[A-Za-z0-9+.\-]+://', url):
        url = 'tcp://{0}'.format(url)
    return urllib.parse.urlparse(url)


def get_next_path(paths):
    """"
    Description: Function to get the path at the required level of the URL from the parsed paths list.
    If path doesn't exist then return none(null).
    :parameter: Parsed Path list.
    :return: Parsed Path level of URL.
    """

    if paths:
        return paths.pop(0)

    return None


def get_url_levels(url):
    """"
    Description: Function to parse the url into different levels of information.
    :parameter: URL
    :return: Returns the parsed url at level1, level2 and level3 as dictionary.
    """

    parsed_url = urlparse(url)

    # Splitting the parsed url path based on '/' into a list of paths.
    all_paths = parsed_url.path.split("/")[1:]

    return {
        "url_level1": parsed_url.netloc,
        "url_level2": get_next_path(all_paths),
        "url_level3": get_next_path(all_paths)
    }


def parse_event(event):
    """"
    Description: Function to parse the event and get the required fields from the event
    and perform transformations on fields where required.
    :parameter: Event row as Json.
    :return: Returns the parsed event as a Dictionary
    with the required fields: user_id, time_stamp, url_level1, url_level2, url_level3, activity.
    """

    # calling function to parse url into required levels and get a dictionary with url levels.
    url_levels = get_url_levels(event["url"])

    parsed_event = {
        "user_id": event["user"]["id"],
        "time_stamp": event["timestamp"],
        "url_level1": url_levels["url_level1"],
        "url_level2": url_levels["url_level2"],
        "url_level3": url_levels["url_level3"],
        "activity": event["action"]
    }
    
    return parsed_event
