import config

import os

# Batch helper script to run batchRunner for stored symbols in config.json
# Can be run to populate symbol data up to this point

rtSymbols = config.settings_json['realtimeSymbols']

curFileDir = os.path.dirname(os.path.realpath(__file__))

# Run job for symbols
for symbols in rtSymbols:
    symbol = symbols['symbol']
    resolutions = symbols['resolutions']
    for timeframe in resolutions:
        options = f"{symbol} -r {timeframe}"
        os.system(f'python3 {curFileDir}/batchRunner.py {options}')
