from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
import numpy as np
from math import pi,sqrt
from pandas.io.json import json_normalize
import json
import os
import random
import string
import decimal
import hashlib


role = 'ngr_exact_sciences'
database = 'ngr_exact_sciences'

ctx_id = snowflake.connector.connect(
    user = 'tcaouette',
    account = "om1id",
    authenticator = 'externalbrowser',
    role = role,
    database = database,
    warehouse = 'LOAD_WH',
    autocommit = False
    )

cs_id = ctx_id.cursor()



