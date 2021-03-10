from datetime import datetime
import logging
import os

# Logging #
DIR = os.path.realpath(os.path.dirname(__file__))
parent = os.path.dirname(DIR)

# Today's Date #
date_today = datetime.today().strftime("%Y-%m-%d")
named_date_fmt = "%B %d, %Y"

DELIM = ","
