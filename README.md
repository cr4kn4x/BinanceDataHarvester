# Binance Kline Data Harvester

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)


A robust and efficient tool for harvesting historical kline (candlestick) data from Binance across multiple markets and storing it in MongoDB for algorithmic trading applications.

## 🚀 Features

- **Multi-Market Support**: Download kline data from Binance Spot, USD-M Futures (UM), and Coin-M Futures (CM) markets
- **Comprehensive Time Intervals**: Support for all Binance kline intervals from 1 second to 1 day
- **MongoDB Integration**: Efficient storage with automatic duplicate handling and resume capability
- **Smart Cursor Management**: Automatically resumes from the last downloaded data point
- **ZIP Cache System**: Option to cache downloaded ZIP files to avoid re-downloading
- **Robust Error Handling**: Graceful handling of network errors and missing data
- **Progress Tracking**: Detailed logging with progress indicators
- **Validation**: Built-in symbol and interval validation for each market type

## 📦 Installation

1. **Clone the repository**:
   ```bash
   https://github.com/cr4kn4x/BinanceDataHarvester.git
   cd BinanceSpotDataHarvester
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your MongoDB connection details and storage paths
   ```

## ⚙️ Configuration

Create a `.env` file with the following required variables:

```env
# MongoDB Connection
MONGO_URI='mongodb://localhost:27017/?authSource=admin'

# Database Names
binance_spot_klines_db='binance_spot_klines'
binance_um_klines_db='binance_um_klines'
binance_cm_klines_db='binance_cm_klines'

# Storage Directory
BASE_DIR='E:/AlgorithmicTrading/crypto/binance'
```

## 🎯 Usage

### Basic Usage

```bash
python main.py --market spot --symbols BTCUSDT ETHUSDT --interval 1m
```

### Advanced Usage

```bash
python main.py \
  --market um \
  --symbols BTCUSDT ETHUSDT ADAUSDT \
  --interval 5m \
  --env-file ./custom.env \
  --log-level debug \
  --use-zip-cache
```

### Command Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--market` | ✅ | Market type: `spot`, `um` (USD-M Futures), `cm` (Coin-M Futures) |
| `--symbols` | ✅ | Space-separated list of trading symbols (e.g., BTCUSDT ETHUSDT) |
| `--interval` | ✅ | Kline interval (see supported intervals below) |
| `--env-file` | ❌ | Path to environment file (default: `./.env`) |
| `--log-level` | ❌ | Log level: `debug`, `info`, `warning` (default: `info`) |
| `--use-zip-cache` | ❌ | Use cached ZIP files instead of re-downloading |

### Supported Intervals

**Spot Market**: `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`

**USD-M & Coin-M Futures**: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`

## 📊 Data Structure

The harvester stores OHLCV (Open, High, Low, Close, Volume) data in MongoDB with the following schema:

```json
{
  "open_time": 1672531200000,
  "open": 16625.10,
  "high": 16649.99,
  "low": 16580.01,
  "close": 16601.23,
  "volume": 1234.5678,
  "close_time": 1672531259999,
  "quote_asset_volume": 20532615.47,
  "number_of_trades": 45231,
  "taker_buy_base_asset_volume": 678.9012,
  "taker_buy_quote_asset_volume": 11283456.78,
  "ignore": 0
}
```

### Database Organization

- **Collections**: Named as `{SYMBOL}_{INTERVAL}` (e.g., `BTCUSDT_1m`)
- **Databases**: Separate databases for each market type
- **Indexing**: Unique index on `open_time` for efficient querying and duplicate prevention

## 🔧 Technical Details

### Architecture

- **Modular Design**: Clean separation of concerns with dedicated modules for:
  - `DAO.py`: Database operations and MongoDB interaction
  - `SpotKlines.py`: Core harvesting logic and data processing
  - `ArgparserValidation.py`: Command-line argument validation
  - `DataStructures.py`: Pydantic models for type safety
  - `utility.py`: Helper functions and Binance API utilities

### Key Features

- **Resume Capability**: Automatically detects the last stored data point and resumes from there
- **Data Integrity**: Uses upsert operations to handle duplicates gracefully
- **Progress Tracking**: Real-time progress indicators showing days processed
- **Error Recovery**: Robust handling of network timeouts, missing data, and API limits
- **Memory Efficient**: Processes data in daily chunks to minimize memory usage

### Data Source

Data is sourced from [Binance Vision](https://data.binance.vision/), Binance's official historical data repository, ensuring:
- ✅ High data quality and accuracy
- ✅ Complete historical coverage
- ✅ Official Binance data integrity
- ✅ No API rate limits for historical data

## 📈 Example Use Cases

1. **Backtesting Trading Strategies**: Complete historical data for strategy validation
2. **Market Analysis**: Analyze price patterns and market behavior
3. **Research & Development**: Academic research on cryptocurrency markets
4. **Portfolio Optimization**: Historical performance analysis
5. **Risk Management**: Calculate historical volatility and risk metrics

## 🛠️ Development

### Running Tests

The project includes a gap detection utility for data integrity verification:

```bash
python test.py
```

This will check for any missing kline data in your MongoDB collections.

### Project Structure

```
BinanceSpotDataHarvester/
├── main.py                 # Entry point
├── requirements.txt        # Python dependencies
├── test.py                # Data integrity testing
└── lib/
    ├── ArgparserValidation.py  # CLI argument parsing and validation
    ├── DAO.py                  # Database access object
    ├── DataStructures.py       # Pydantic data models
    ├── SpotKlines.py           # Core harvesting logic
    ├── Types.py                # Type definitions
    └── utility.py              # Helper functions and utilities
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License
- TBD

## 🙏 Acknowledgments

- [Binance](https://www.binance.com/) for providing comprehensive historical data
- [Binance Vision](https://data.binance.vision/) for the official data repository
- The Python community for excellent libraries like `pymongo`, `pydantic`, and `requests`

## ⚠️ Disclaimer

This tool is for educational and research purposes. Please ensure you comply with Binance's Terms of Service and any applicable regulations in your jurisdiction.