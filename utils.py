import datetime

def GetDateTimeAsFileName():
    """
    Get the current date and time as a formatted string.
    
    Returns:
        str: Current date and time in the format 'MMDD_HHMM'
    """
    now = datetime.datetime.now()
    return now.strftime("%m%d_%H%M")

