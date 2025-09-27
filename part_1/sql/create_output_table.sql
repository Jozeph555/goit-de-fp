CREATE TABLE IF NOT EXISTS enriched_athlete_stats (
    sport VARCHAR(100),
    medal VARCHAR(20),
    sex CHAR(1),
    country_noc VARCHAR(3),
    avg_height DECIMAL(5,2),
    avg_weight DECIMAL(5,2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sport, medal, sex, country_noc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;