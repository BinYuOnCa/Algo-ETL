"""
This script is to store the related parameters when connecting to TWS.
(Tested based on paper account)
"""

# ----- Account Setting -----
ACCOUNT = "DU1234567"  # The IB account number
IBIP = "127.0.0.1"     # The server IP, local default: 127.0.0.1
PORT = 7497            # IB port: simulation default: 7497
TEST_MODE = False

# ----- Money Check -----
INVESTMENT_AMT = 5000
TOTAL_BUY_POWER = 60000

# ----- Function Name -----
BUYING_POWER_KEY = "BuyingPower"
VWAP_WINDOW = '30 D'
VWAP_UNIT = '1 day'

# ----- Environment -----
TIME_ZONE_ADJ = 0