CREATE TABLE IF NOT EXISTS dim_currency (
  currency_key INTEGER PRIMARY KEY,
  symbol TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_rates (
  date DATE,
  base TEXT,
  currency_key INTEGER REFERENCES dim_currency(currency_key),
  rate DOUBLE
);
