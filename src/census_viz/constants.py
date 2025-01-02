"""Constants used throughout the census-viz package"""

# State centers (lat, lon) for map visualization
STATE_CENTERS = {
    "01": (32.7794, -86.8287),  # Alabama
    "02": (64.0685, -152.2782),  # Alaska
    "04": (34.2744, -111.6602),  # Arizona
    "05": (34.8938, -92.4426),  # Arkansas
    "06": (37.1841, -119.4696),  # California
    "08": (38.9972, -105.5478),  # Colorado
    "09": (41.6219, -72.7273),  # Connecticut
    "10": (38.9896, -75.5050),  # Delaware
    "11": (38.9101, -77.0147),  # District of Columbia
    "12": (28.6305, -82.4497),  # Florida
    "13": (32.6415, -83.4426),  # Georgia
    "15": (20.2927, -156.3737),  # Hawaii
    "16": (44.3509, -114.6130),  # Idaho
    "17": (40.0417, -89.1965),  # Illinois
    "18": (39.8942, -86.2816),  # Indiana
    "19": (42.0751, -93.4960),  # Iowa
    "20": (38.4937, -98.3804),  # Kansas
    "21": (37.5347, -85.3021),  # Kentucky
    "22": (31.0689, -91.9968),  # Louisiana
    "23": (45.3695, -69.2428),  # Maine
    "24": (39.0550, -76.7909),  # Maryland
    "25": (42.2596, -71.8083),  # Massachusetts
    "26": (44.3467, -85.4102),  # Michigan
    "27": (46.2807, -94.3053),  # Minnesota
    "28": (32.7364, -89.6678),  # Mississippi
    "29": (38.3566, -92.4580),  # Missouri
    "30": (47.0527, -109.6333),  # Montana
    "31": (41.5378, -99.7951),  # Nebraska
    "32": (39.3289, -116.6312),  # Nevada
    "33": (43.6805, -71.5811),  # New Hampshire
    "34": (40.1907, -74.6728),  # New Jersey
    "35": (34.4071, -106.1126),  # New Mexico
    "36": (42.9538, -75.5268),  # New York
    "37": (35.5557, -79.3877),  # North Carolina
    "38": (47.4501, -100.4659),  # North Dakota
    "39": (40.2862, -82.7937),  # Ohio
    "40": (35.5889, -97.4943),  # Oklahoma
    "41": (43.9336, -120.5583),  # Oregon
    "42": (40.8781, -77.7996),  # Pennsylvania
    "44": (41.6762, -71.5562),  # Rhode Island
    "45": (33.9169, -80.8964),  # South Carolina
    "46": (44.4443, -100.2263),  # South Dakota
    "47": (35.8580, -86.3505),  # Tennessee
    "48": (31.4757, -99.3312),  # Texas
    "49": (39.3055, -111.6703),  # Utah
    "50": (44.0687, -72.6658),  # Vermont
    "51": (37.5215, -78.8537),  # Virginia
    "53": (47.3826, -120.4472),  # Washington
    "54": (38.6409, -80.6227),  # West Virginia
    "55": (44.6243, -89.9941),  # Wisconsin
    "56": (42.9957, -107.5512),  # Wyoming
}

# Default center point for the continental US
DEFAULT_CENTER = (39.8283, -98.5795)  # Geographic center of the lower 48 states

# Demographic variables mapping
DEMOGRAPHIC_VARS = {
    "total": "total_population",
    "under18": "under_18",
    "under5": "under_5",
    "5to9": "age_5_to_9",
    "10to14": "age_10_to_14",
    "15to17": "age_15_to_17",
    "school_age": "school_age",
    "income": "median_income",
}
