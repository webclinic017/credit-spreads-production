from pandas_market_calendars import get_calendar

def schedule_trading_dates(exchange, start_date, end_date) -> list[str]:
    """Produces list of available trading dates for a given exchange"""
    calendar = get_calendar(exchange)
    return calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d").values
