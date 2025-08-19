# Java Snowpipe Streaming Demo

Simple setup for high-performance Snowpipe Streaming demonstrations.

## Quick Start

```bash
# Test connection
java -jar CDCSimulatorClient.jar TEST

# Demo streaming (100 TPS - perfect for demos)
java -jar CDCSimulatorClient.jar SLOOW

# Use Ctrl+C to stop streaming
```

## Scripts Available

- **`Test.sh/bat`** - Quick connection test
- **`Run_Sloow.sh/bat`** - Demo streaming at 100 TPS  
- **`Build.sh/bat`** - Rebuild JAR if needed

## Configuration

Edit `snowflake.properties` for your Snowflake account details.

**That's it!** Simple and focused for demonstrations. 🎯
